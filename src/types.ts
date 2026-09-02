import { DataQuery, DataSourceJsonData } from '@grafana/data';

/**
 * Arc datasource configuration options
 */
/**
 * Wire protocol for Arc query responses.
 * - arrow: Apache Arrow IPC — fastest decode path, the default
 * - msgpack: columnar MessagePack — stable since Arc 26.09.1, ~78% of
 *   Arrow's throughput, supports SHOW statements and zstd/gzip compression
 * - json: compatibility fallback
 */
export type ArcProtocol = 'arrow' | 'msgpack' | 'json';

export interface ArcDataSourceOptions extends DataSourceJsonData {
  url?: string;
  database?: string;
  timeout?: number;
  protocol?: ArcProtocol;
  /**
   * Legacy toggle superseded by `protocol`. Kept so datasources saved by
   * older plugin versions keep their JSON/Arrow choice, and written on
   * protocol changes so a plugin downgrade still honors the selection.
   */
  useArrow?: boolean;
  maxConcurrency?: number;
  /**
   * Per-response body size cap in MiB. Default 1024 MiB. Defense-in-depth
   * against runaway queries that would OOM the plugin process. Raise this
   * for very large analytical queries (Arc emits "Arrow IPC stream
   * truncated after headers committed" when the cap is hit mid-stream).
   */
  maxResponseMB?: number;
  /**
   * Permit the configured Arc URL to resolve to a private/RFC1918 address.
   * Off by default — the SSRF guard blocks private ranges to protect Grafana
   * installs where datasource creators are not fully trusted. Enable when
   * Arc is deployed on an internal corporate network.
   */
  allowPrivateIPs?: boolean;
  /**
   * Permit per-query `database` field to override the datasource default.
   * Off by default — without this, a dashboard editor could switch databases
   * on a datasource the admin configured for a single tenant (confused-deputy
   * if the Arc API key has cross-database scope). Enable only when the API
   * key's authorization scope matches the dashboard-editor's authorization.
   */
  allowDatabaseOverride?: boolean;
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
  rawSql?: string; // Postgres/MySQL/MSSQL/ClickHouse compatibility
  splitDuration?: string; // "off", "1h", "6h", "12h", "1d", "3d", "7d"
  database?: string; // Per-query database override (empty = use datasource default)
}

/**
 * Default values
 */
export const defaultQuery: Partial<ArcQuery> = {
  sql: 'SELECT * FROM cpu WHERE $__timeFilter(time) LIMIT 100',
  format: 'time_series',
  rawQuery: true,
};
