import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

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
  /** Max parallel chunks WITHIN one split query. Default 4. */
  maxConcurrency?: number;
  /**
   * Max simultaneous Arc requests for this datasource, across every panel and
   * viewer. Default 32. Distinct from maxConcurrency, which shapes a single
   * query's fan-out; this one protects Arc and the plugin process.
   */
  maxInFlight?: number;
  /**
   * Per-response body size cap in MiB. Default 1024 MiB. Defense-in-depth
   * against runaway queries that would OOM the plugin process. Raise this
   * for very large analytical queries (Arc emits "Arrow IPC stream
   * truncated after headers committed" when the cap is hit mid-stream).
   */
  maxResponseMB?: number;
  /**
   * Permit the configured Arc URL to resolve to a private/RFC1918 address.
   * On by default: self-hosted Arc usually runs on a private network or a
   * Docker service name. Turn it off to require a public address, on installs
   * where datasource creators are not fully trusted. Link-local and
   * cloud-metadata addresses are blocked either way.
   */
  allowPrivateIPs?: boolean;
  /**
   * Permit a per-query `database` field to override the datasource default.
   * On by default, matching the per-query override this plugin has supported
   * since 1.1.0. Turn it off when the Arc API key spans more databases than
   * dashboard editors should reach, since an editor could otherwise point a
   * panel at a database the admin did not configure.
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
