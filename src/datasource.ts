import {
  DataQueryResponse,
  MetricFindValue,
  DataSourceInstanceSettings,
  CoreApp,
  ScopedVars,
  VariableWithMultiSupport,
} from '@grafana/data';
import { frameToMetricFindValue, DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { ArcQuery, ArcDataSourceOptions, defaultQuery } from './types';
import { lastValueFrom } from 'rxjs';

/**
 * Arc DataSource - extends DataSourceWithBackend to automatically handle
 * all backend communication and frame parsing
 */
export class ArcDataSource extends DataSourceWithBackend<ArcQuery, ArcDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<ArcDataSourceOptions>) {
    super(instanceSettings);
  }

  /**
   * Query for template variables
   */
  async metricFindQuery(query: any, options?: any): Promise<MetricFindValue[]> {
    // Handle both string SQL and query object
    let sqlQuery: string;

    if (typeof query === 'string') {
      // Simple string query
      sqlQuery = query;
    } else if (query && typeof query === 'object') {
      // Query object - extract SQL from various possible field names
      sqlQuery = query.sql || query.query || query.rawSql || '';

      // Log to help debug
      if (!sqlQuery) {
        console.warn('metricFindQuery received object without sql:', query);
      }
    } else {
      sqlQuery = '';
    }

    const target: ArcQuery = {
      refId: 'metricFindQuery',
      sql: sqlQuery,
      format: 'table',
    };

    return lastValueFrom(
      super.query({
        ...(options ?? {}), // includes 'range'
        targets: [target],
      })
    ).then(this.toMetricFindValue);
  }

  toMetricFindValue(rsp: DataQueryResponse): MetricFindValue[] {
    const data = rsp.data ?? [];
    // Create MetricFindValue object for all frames
    const values = data.map((d) => frameToMetricFindValue(d)).flat();
    // Filter out duplicate elements
    return values.filter((elm, idx, self) => idx === self.findIndex((t) => t.text === elm.text));
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
