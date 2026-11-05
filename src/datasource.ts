import {
  DataQueryRequest,
  TimeRange,
  dateTime,
  DataQueryResponse,
  MetricFindValue,
  DataSourceInstanceSettings,
  CoreApp,
  ScopedVars,
  VariableWithMultiSupport,
  LegacyMetricFindQueryOptions,
} from '@grafana/data';
import { frameToMetricFindValue, DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { ArcQuery, ArcDataSourceOptions, BuilderQuery, Filter, builderDefaultQuery, emptyBuilder } from './types';
import { escapeLiteral, quoteLiteral, resolveTimezone } from './interpolation';
import { resolveMode } from './builder/mode';
import { builderFormat, rawSingle, VariableUseError } from './builder/interpolate';
import { MEASUREMENT_RE, valuesSQL, variableName } from './builder/generate';
import { VariableBuilder, resolveVariableMode, toVariableQuery } from './builder/variableQuery';
import { kindOf } from './builder/columns';
import { lastValueFrom, Observable } from 'rxjs';

/** The backend's database-name rule (pkg/plugin/safety.go databaseNameRe). */
const DATABASE_RE = /^[A-Za-z0-9_-]+$/;

/** Arc's SHOW TABLES / SHOW MEASUREMENTS, which list `database` first. */
const SHOW_TABLES_RE = /^\s*SHOW\s+(TABLES|MEASUREMENTS)\b/i;

/**
 * Grafana takes a variable's values from the first text column. Arc's SHOW
 * TABLES puts `database` there, so every row read as the database name and
 * the variable collapsed to one value. Moves `table_name` to the front.
 */
export function tableNameFirst(rsp: DataQueryResponse): DataQueryResponse {
  const data = (rsp.data ?? []).map((frame) => {
    const fields = frame?.fields;
    const i = Array.isArray(fields) ? fields.findIndex((f) => f.name === 'table_name') : -1;
    if (i <= 0) {
      return frame;
    }
    return { ...frame, fields: [fields[i], ...fields.slice(0, i), ...fields.slice(i + 1)] };
  });
  return { ...rsp, data };
}

/**
 * Shapes a `metricFindQuery` argument can arrive as. Grafana's
 * `DataSourceApi.metricFindQuery` is typed as `(query: any, ...)` upstream so
 * it can't narrow for us; this is the set we accept in practice.
 *  - plain string: the SQL text directly
 *  - object: legacy variable-query shapes from other datasources we want to
 *    interoperate with (Postgres, MySQL, MSSQL all use `rawSql`; the Arc-
 *    native shape uses `sql`)
 */
type VariableQueryInput = string | { sql?: string; query?: string; rawSql?: string } | null | undefined;

/**
 * Arc DataSource - extends DataSourceWithBackend to automatically handle
 * all backend communication and frame parsing
 */
export class ArcDataSource extends DataSourceWithBackend<ArcQuery, ArcDataSourceOptions> {
  private readonly options: ArcDataSourceOptions;

  constructor(instanceSettings: DataSourceInstanceSettings<ArcDataSourceOptions>) {
    super(instanceSettings);
    this.options = instanceSettings.jsonData ?? {};
  }

  /**
   * Query for template variables. Accepts both string SQL and an object
   * containing one of `sql`/`query`/`rawSql` (the latter two for cross-
   * datasource compatibility — Postgres/MySQL/MSSQL/ClickHouse all use
   * `rawSql`).
   */
  async metricFindQuery(query: VariableQueryInput, options?: LegacyMetricFindQueryOptions): Promise<MetricFindValue[]> {
    const vq = toVariableQuery(query);
    if (vq.builder && resolveVariableMode(vq) === 'builder') {
      return this.builderVariableValues(vq.builder, options);
    }
    const sqlQuery = extractVariableSQL(query);

    const target: ArcQuery = {
      refId: 'metricFindQuery',
      sql: sqlQuery,
      format: 'table',
    };

    // Build a DataQueryRequest by spreading the variable-query options
    // (range, scopedVars, etc. carried over from Grafana) and adding our
    // single target. LegacyMetricFindQueryOptions is structurally compatible
    // with DataQueryRequest minus the `targets` field; the cast names the
    // target type explicitly so future maintainers see what shape is being
    // produced (was `as never`, which preserved no type info).
    const request = { ...(options ?? {}), targets: [target] } as unknown as DataQueryRequest<ArcQuery>;
    // `this.query`, not `super.query`: the override stamps the dashboard
    // timezone, so a variable query using $__timeGroup buckets the same way a
    // panel query on the same dashboard does.
    const rsp = await lastValueFrom(this.query(request));
    return this.toMetricFindValue(SHOW_TABLES_RE.test(sqlQuery) ? tableNameFirst(rsp) : rsp);
  }

  /**
   * A builder variable's values. Databases, measurements and columns come
   * from the metadata routes: they work on every protocol and fall back to
   * the configured database for a key that cannot list them all. Values run
   * as builder SQL, so a chained variable becomes a quoted literal.
   */
  private async builderVariableValues(
    b: VariableBuilder,
    options?: LegacyMetricFindQueryOptions
  ): Promise<MetricFindValue[]> {
    const scopedVars = options?.scopedVars ?? {};
    const db = b.database ? interpolateName(b.database, scopedVars, 'database', DATABASE_RE) : '';
    const texts = (names: string[]) => names.map((text) => ({ text }));
    switch (b.kind) {
      case 'databases':
        return texts((await this.getDatabases()).databases);
      case 'measurements':
        return texts(await this.getMeasurements(db));
      case 'columns': {
        const m = interpolateName(b.measurement ?? '', scopedVars, 'measurement', MEASUREMENT_RE);
        const res = await this.getColumns(db, m);
        return texts(
          (res.columns ?? [])
            .filter((c) => c.name !== 'time')
            .filter((c) => b.textOnly === false || ['string', 'boolean'].includes(kindOf(c.type)))
            .map((c) => c.name)
        );
      }
      case 'values': {
        if (!b.sql) {
          return [];
        }
        const target: ArcQuery = {
          refId: 'metricFindQuery',
          sql: b.sql,
          format: 'table',
          splitDuration: 'off',
          editorMode: 'builder',
          builder: { ...emptyBuilder(), measurement: b.measurement, sql: b.sql },
          ...(b.database ? { database: b.database } : {}),
        };
        const request = { ...(options ?? {}), targets: [target] } as unknown as DataQueryRequest<ArcQuery>;
        const rsp = await lastValueFrom(this.query(request));
        if (rsp.errors?.length) {
          throw new Error(rsp.errors[0].message ?? 'variable query failed');
        }
        return this.toMetricFindValue(rsp);
      }
    }
  }

  toMetricFindValue(rsp: DataQueryResponse): MetricFindValue[] {
    const data = rsp.data ?? [];
    const values = data.map((d) => frameToMetricFindValue(d)).flat();
    // Dedup by `.text` in a single linear pass via Set lookup — was
    // O(N²) via findIndex inside filter (R1 M25). Order-preserving.
    const seen = new Set<string>();
    const out: MetricFindValue[] = [];
    for (const v of values) {
      const key = String(v.text);
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      out.push(v);
    }
    return out;
  }

  /**
   * New queries open in the visual builder. Grafana also merges this over
   * existing queries (alert rules, datasource switches), which is why the
   * editor decides the mode with resolveMode rather than by `editorMode`.
   */
  getDefaultQuery(_: CoreApp): Partial<ArcQuery> {
    return builderDefaultQuery();
  }

  /**
   * Skips builder queries that are not complete yet (their `sql` is empty),
   * so a dashboard refresh does not send them. Never skips a query that has
   * SQL, or one migrated from another SQL datasource (`rawSql`).
   */
  filterQuery(query: ArcQuery): boolean {
    if (resolveMode(query) === 'builder' && !(query.sql ?? '').trim()) {
      return false;
    }
    return true;
  }

  /** Whether the datasource lets a query pick another database. */
  allowsDatabaseOverride(): boolean {
    return this.options.allowDatabaseOverride ?? true;
  }

  /** The datasource's configured database. */
  defaultDatabase(): string {
    return this.options.database || 'default';
  }

  /**
   * Formats a template-variable value for interpolation into SQL.
   *
   * This is a deliberate, verbatim match of Grafana's own SQL datasources
   * (`packages/grafana-sql/src/datasource/SqlDatasource.ts`). Do not change it
   * without diffing against that file first.
   *
   * The rule: quote ONLY when the variable is multi-value or has an "All"
   * option; otherwise escape embedded quotes and leave the value bare. It
   * looks asymmetric but both halves are load-bearing:
   *
   *   - Single-value variables are written inside the author's own quotes
   *     (`WHERE host = '$server'`, `host ~ '^$server$'`). Adding a second pair
   *     yields `''h01''` / `'^'h01'$'` — a parser error, not extra safety.
   *   - Multi-value variables land where the author cannot pre-quote each
   *     element (`WHERE cpu = $cpu`, `host IN ($hosts)`), so each element is
   *     quoted here.
   *
   * A previous release (1.3.2, finding R2-HI5) quoted unconditionally on the
   * premise that "the Postgres datasource quotes unconditionally". That
   * premise is false — upstream has quoted only multi/All since the plugin was
   * written — and acting on it broke every dashboard using the quoted idiom.
   * Doubling embedded quotes is the half that actually prevents a URL-supplied
   * value from terminating the literal and appending SQL; that is retained on
   * both paths. A bare `= $var` remains injectable exactly as it is in
   * Grafana's own Postgres/MySQL datasources, where Arc's API-key scope is the
   * authorization boundary.
   */
  // `variable` is optional: Grafana routes its own built-in macros ($__interval,
  // $__from, $__to) through this hook without a variable model.
  interpolateVariable = (value: string | string[] | number, variable?: VariableWithMultiSupport) => {
    if (typeof value === 'string') {
      if (variable?.multi || variable?.includeAll) {
        return quoteLiteral(value);
      }
      return escapeLiteral(value);
    }

    if (typeof value === 'number') {
      return value;
    }

    if (Array.isArray(value)) {
      // An empty selection would otherwise join to "", producing `IN ()` —
      // a DuckDB parser error rather than a query that matches nothing.
      // NULL parses and matches nothing, which is what an empty set means.
      if (value.length === 0) {
        return 'NULL';
      }
      const quotedValues = value.map((v) => quoteLiteral(v));
      return quotedValues.join(',');
    }

    return value;
  };

  /**
   * Stamps the dashboard's timezone onto every query before it reaches the
   * backend, so `$__timeGroup` can bucket by local calendar days rather than
   * UTC ones.
   *
   * Done here rather than in applyTemplateVariables because the timezone is a
   * property of the REQUEST, not of an individual target — and because
   * `request.timezone` is only available at this level.
   */
  query(request: DataQueryRequest<ArcQuery>): Observable<DataQueryResponse> {
    const timezone = resolveTimezone(request.timezone);
    return super.query({
      ...request,
      targets: request.targets.map((t) => ({ ...t, timezone })),
    });
  }

  /**
   * Interpolates variables before a query is sent. Builder queries use
   * builderFormat (every value a complete literal); Code queries keep the
   * historical rule above. In both, a variable database is resolved to one
   * name and validated, and builder state is not sent to the backend.
   */
  applyTemplateVariables(query: ArcQuery, scopedVars: ScopedVars): ArcQuery {
    const { builder, ...rest } = query;
    const templateSrv = getTemplateSrv();
    const out: ArcQuery = { ...rest };
    if (query.database) {
      out.database = interpolateName(query.database, scopedVars, 'database', DATABASE_RE);
    }
    if (resolveMode(query) === 'builder') {
      const m = builder?.measurement;
      if (m && variableName(m) !== undefined) {
        interpolateName(m, scopedVars, 'measurement', MEASUREMENT_RE);
      }
      out.sql = templateSrv.replace(query.sql, scopedVars, builderFormat);
    } else {
      out.sql = templateSrv.replace(query.sql, scopedVars, this.interpolateVariable);
    }
    return out;
  }

  /**
   * Used by "New alert rule" from a panel (and Explore). Alert evaluation
   * never interpolates variables, so the rule stores the SQL with today's
   * values, as Code: the builder state no longer matches it.
   */
  interpolateVariablesInQueries(queries: ArcQuery[], scopedVars: ScopedVars): ArcQuery[] {
    return queries.map((q) => {
      const out = this.applyTemplateVariables(q, scopedVars);
      return { ...out, editorMode: 'code' as const };
    });
  }

  /** Query builder metadata (backend routes in pkg/plugin/resources.go). */
  async getDatabases(): Promise<{ databases: string[]; scoped: boolean }> {
    return this.getResource('databases');
  }

  async getMeasurements(database: string): Promise<string[]> {
    const res = await this.getResource('measurements', database ? { database } : {});
    return res.measurements ?? [];
  }

  /**
   * Distinct values of `column` for a filter's value dropdown: at most
   * `limit`, `more` set when there are others. The SQL comes from the
   * builder's generator, so the other filters apply exactly as in the query,
   * and it runs through the normal query path on the datasource's own
   * protocol, over `range` (the panel's time range).
   */
  async getValues(
    builder: BuilderQuery,
    database: string,
    column: string,
    otherFilters: Filter[],
    search: string,
    range: TimeRange | undefined,
    limit = 100
  ): Promise<{ values: string[]; more: boolean }> {
    const sql = valuesSQL(builder, column, otherFilters, search, limit);
    if (!sql) {
      return { values: [], more: false };
    }
    const to = dateTime();
    const r: TimeRange = range ?? { from: dateTime(to).subtract(1, 'hour'), to, raw: { from: 'now-1h', to: 'now' } };
    const target: ArcQuery = {
      refId: 'builderValues',
      sql,
      format: 'table',
      splitDuration: 'off',
      // Marked as builder SQL so variables in the other filters are
      // interpolated the way the builder query itself will be.
      editorMode: 'builder',
      builder: { ...emptyBuilder(), measurement: builder.measurement, sql },
      ...(database ? { database } : {}),
    };
    const request = {
      targets: [target],
      range: r,
      scopedVars: {},
      interval: '1m',
      intervalMs: 60000,
    } as unknown as DataQueryRequest<ArcQuery>;
    const rsp = await lastValueFrom(this.query(request));
    if (rsp.errors?.length) {
      throw new Error(rsp.errors[0].message ?? 'value lookup failed');
    }
    const values = this.toMetricFindValue(rsp).map((v) => String(v.text));
    return { values: values.slice(0, limit), more: values.length > limit };
  }

  async getColumns(database: string, measurement: string): Promise<ColumnsResponse> {
    const params: Record<string, string> = { measurement };
    if (database) {
      params.database = database;
    }
    return this.getResource('columns', params);
  }
}

export interface ColumnInfo {
  name: string;
  type: string;
}

export interface ColumnsResponse {
  columns: ColumnInfo[];
  /** schema: Arc's stored schema; describe: inferred from recent data; none: unknown. */
  source: 'schema' | 'describe' | 'none';
  message?: string;
}

/**
 * Resolves a database or measurement that may be a variable to one name and
 * checks it. A multi-value selection or a name the backend would refuse is an
 * error naming the variable, rather than a confusing failure from Arc.
 */
function interpolateName(raw: string, scopedVars: ScopedVars, what: string, re: RegExp): string {
  if (!raw.includes('$')) {
    return raw;
  }
  let value: string;
  try {
    value = getTemplateSrv().replace(raw, scopedVars, rawSingle);
  } catch (e) {
    if (e instanceof VariableUseError) {
      throw new Error(`The ${what} variable ${raw}: ${e.message}`);
    }
    throw e;
  }
  if (!re.test(value)) {
    throw new Error(`The ${what} variable ${raw} resolved to "${value}", which is not a valid ${what} name`);
  }
  return value;
}

/**
 * Extracts SQL text from a `metricFindQuery` argument, handling every
 * variable-query shape Grafana datasources have used historically. Returns
 * an empty string if no SQL is present (Grafana will surface "no data"
 * rather than the plugin throwing).
 */
function extractVariableSQL(query: VariableQueryInput): string {
  if (typeof query === 'string') {
    return query;
  }
  if (query && typeof query === 'object') {
    return query.sql ?? query.query ?? query.rawSql ?? '';
  }
  return '';
}
