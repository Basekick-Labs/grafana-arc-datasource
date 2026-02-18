import { DataQuery, DataSourceJsonData } from '@grafana/data';

/**
 * Arc datasource configuration options
 */
export interface ArcDataSourceOptions extends DataSourceJsonData {
  url?: string;
  database?: string;
  timeout?: number;
  useArrow?: boolean;
}

/**
 * Secure configuration (API key stored encrypted)
 */
export interface ArcSecureJsonData {
  apiKey?: string;
}

/**
 * Arc query model
 */
export interface ArcQuery extends DataQuery {
  sql: string;
  format?: 'time_series' | 'table';
  rawQuery?: boolean;
  splitDuration?: string; // "off", "1h", "6h", "12h", "1d", "3d", "7d"
}

/**
 * Default values
 */
export const defaultQuery: Partial<ArcQuery> = {
  sql: 'SELECT * FROM cpu WHERE $__timeFilter(time) LIMIT 100',
  format: 'time_series',
  rawQuery: true,
};
