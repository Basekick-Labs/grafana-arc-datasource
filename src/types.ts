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
  /** The only thing the backend executes, in both editor modes. */
  sql: string;
  format?: 'time_series' | 'table';
  /**
   * Which editor the query was last committed from. Absent on every query
   * saved before the visual builder existed; those open in Code. Never trust
   * this alone: see resolveMode in builder/mode.ts.
   */
  editorMode?: EditorMode;
  /** Visual builder state (builder/). Ignored by the backend; stripped from requests. */
  builder?: BuilderQuery;
  /** Read by nothing. Kept so saved queries that carry it still type-check. */
  rawQuery?: boolean;
  rawSql?: string; // Postgres/MySQL/MSSQL/ClickHouse compatibility
  splitDuration?: string; // "off", "1h", "6h", "12h", "1d", "3d", "7d"
  database?: string; // Per-query database override (empty = use datasource default)
  /**
   * IANA name of the dashboard's timezone, resolved on the frontend (where
   * Grafana's "browser" / "utc" / explicit setting is known) and sent to the
   * backend so $__timeGroup can bucket by local calendar days. The backend
   * falls back to UTC when this is absent or unrecognised.
   */
  timezone?: string;
}

export type EditorMode = 'builder' | 'code';

/** Functions the builder offers, InfluxQL names. See builder/functions.ts for the SQL. */
export type AggregateFn =
  | 'raw'
  | 'mean'
  | 'median'
  | 'sum'
  | 'count'
  | 'count_rows'
  | 'count_distinct'
  | 'min'
  | 'max'
  | 'first'
  | 'last'
  | 'stddev'
  | 'spread'
  | 'percentile';

export type FilterOp =
  | '='
  | '!='
  | '<'
  | '<='
  | '>'
  | '>='
  | '=~'
  | '!~'
  | 'LIKE'
  | 'NOT LIKE'
  | 'IS NULL'
  | 'IS NOT NULL';

/** How a filter's value is written as a literal. Chosen from the column's type when the filter is made. */
export type FilterValueType = 'string' | 'number' | 'boolean';

export type TransformKind =
  | 'derivative'
  | 'non_negative_derivative'
  | 'difference'
  | 'non_negative_difference'
  | 'moving_average'
  | 'cumulative_sum';

/** An InfluxQL transformation applied to a select row's values, per series, in time order. */
export interface Transform {
  kind: TransformKind;
  /** derivative kinds: the rate's unit, `<n><unit>` with ns, us, ms, s, m, h, d. Default 1s. */
  unit?: string;
  /** moving_average: how many values, 2 to 1000. */
  n?: string;
}

export interface SelectItem {
  /** Column name. Unused for count_rows. */
  column: string;
  fn: AggregateFn;
  /** percentile's n, a decimal lexeme in [0, 100]. */
  param?: string;
  alias?: string;
  transform?: Transform;
  /** Math applied last, as typed: a chain like `* 8 / 1024` (generate.ts parseMath). */
  math?: string;
}

export interface Filter {
  column: string;
  op: FilterOp;
  /** Literal as typed, or a variable reference ($name / ${name}). Unused for IS [NOT] NULL. */
  value?: string;
  valueType?: FilterValueType;
  /** How this filter joins the one before it; the first filter's is ignored. AND binds tighter than OR. */
  connector?: 'AND' | 'OR';
}

export interface BuilderQuery {
  /** Builder model version. */
  version: 1;
  /**
   * Bumped whenever generate() can produce different SQL for the same state,
   * or the state gains fields: an older plugin then opens the query in Code
   * rather than regenerating it without them (mode.ts). 2: transformations,
   * math, auto-suffixed default aliases. 3: fill, OR filters.
   */
  generatorVersion: number;
  /** Fingerprint: exactly the SQL generate() committed. '' while the query is incomplete. */
  sql: string;
  measurement?: string;
  select: SelectItem[];
  where: Filter[];
  groupBy: {
    /** fill: NULL, previous, linear or a number lexeme for $__timeGroup's third argument; unset fills nothing. */
    time?: { interval: string; fill?: string };
    tags: string[];
  };
  orderByTime?: 'ASC' | 'DESC';
  /** Integer lexeme; empty means no LIMIT. */
  limit?: string;
}

/** The generator version new and edited builder queries carry. */
export const GENERATOR_VERSION = 3;

/** A fresh, empty builder state. */
export function emptyBuilder(): BuilderQuery {
  return {
    version: 1,
    generatorVersion: GENERATOR_VERSION,
    sql: '',
    select: [],
    where: [],
    groupBy: { time: { interval: '$__interval' }, tags: [] },
  };
}

/**
 * Default values for queries Code mode creates (and the shape older code
 * expected). New queries get builderDefaultQuery() from getDefaultQuery.
 */
export const defaultQuery: Partial<ArcQuery> = {
  sql: 'SELECT * FROM cpu WHERE $__timeFilter(time) LIMIT 100',
  format: 'time_series',
};

/** What a new query starts as: an empty builder query. */
export function builderDefaultQuery(): Partial<ArcQuery> {
  return { editorMode: 'builder', format: 'time_series', sql: '', builder: emptyBuilder() };
}
