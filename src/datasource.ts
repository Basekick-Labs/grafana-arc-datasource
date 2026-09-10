import {
  DataQueryRequest,
  DataQueryResponse,
  MetricFindValue,
  DataSourceInstanceSettings,
  CoreApp,
  ScopedVars,
  VariableWithMultiSupport,
  LegacyMetricFindQueryOptions,
} from '@grafana/data';
import { frameToMetricFindValue, DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { ArcQuery, ArcDataSourceOptions, defaultQuery } from './types';
import { escapeLiteral, quoteLiteral } from './interpolation';
import { lastValueFrom } from 'rxjs';

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
  constructor(instanceSettings: DataSourceInstanceSettings<ArcDataSourceOptions>) {
    super(instanceSettings);
  }

  /**
   * Query for template variables. Accepts both string SQL and an object
   * containing one of `sql`/`query`/`rawSql` (the latter two for cross-
   * datasource compatibility — Postgres/MySQL/MSSQL/ClickHouse all use
   * `rawSql`).
   */
  async metricFindQuery(query: VariableQueryInput, options?: LegacyMetricFindQueryOptions): Promise<MetricFindValue[]> {
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
    return lastValueFrom(super.query(request)).then(this.toMetricFindValue);
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

  getDefaultQuery(_: CoreApp): Partial<ArcQuery> {
    return defaultQuery;
  }

  quoteLiteral(value: string) {
    return quoteLiteral(value);
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
  interpolateVariable = (value: string | string[] | number, variable: VariableWithMultiSupport) => {
    if (typeof value === 'string') {
      if (variable?.multi || variable?.includeAll) {
        return this.quoteLiteral(value);
      }
      return escapeLiteral(value);
    }

    if (typeof value === 'number') {
      return value;
    }

    if (Array.isArray(value)) {
      const quotedValues = value.map((v) => this.quoteLiteral(v));
      return quotedValues.join(',');
    }

    return value;
  };

  applyTemplateVariables(query: ArcQuery, scopedVars: ScopedVars): ArcQuery {
    return {
      ...query,
      sql: getTemplateSrv().replace(query.sql, scopedVars, this.interpolateVariable),
    };
  }
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
