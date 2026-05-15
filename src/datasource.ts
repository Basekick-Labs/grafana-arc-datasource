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
    return "'" + value.replace(/'/g, "''") + "'";
  }

  interpolateVariable = (value: string | string[] | number, _variable: VariableWithMultiSupport) => {
    if (typeof value === 'string') {
      // R2-HI5: always quote single-value strings. Previously this branch
      // doubled embedded `'` but returned the value without surrounding
      // quotes — `WHERE host = $foo` with `?var-foo=1 OR 1=1--` produced a
      // URL-driven SQL injection with the API key's full scope. Matches the
      // Postgres datasource which quotes unconditionally.
      return this.quoteLiteral(value);
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
