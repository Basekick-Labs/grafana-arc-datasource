/**
 * The query builder's SQL generator (issue #20, plan v3 §4.3).
 *
 * Pure: state in, SQL or errors out. It never looks at the schema, so the
 * same state always yields the same SQL. A column that disappeared is a
 * warning in the editor, not a different query.
 */
import { AggregateFn, BuilderQuery, Filter, FilterOp, SelectItem } from '../types';

export type Format = 'time_series' | 'table';

export interface FieldError {
  /** Which part of the builder the error belongs to, e.g. `select.1.alias`. */
  field: string;
  message: string;
}

export type GenerateResult = { sql: string; errors?: undefined } | { sql?: undefined; errors: FieldError[] };

/** Arc's query-side identifier rule (internal/api/query.go validIdentifierPattern). */
export const MEASUREMENT_RE = /^[a-zA-Z_][a-zA-Z0-9_-]*$/;

/** A variable reference the builder accepts: `$name` or `${name}`, nothing else. */
const VARIABLE_RE = /^\$(?:([A-Za-z_][A-Za-z0-9_]*)|\{([A-Za-z_][A-Za-z0-9_]*)\})$/;

/** Returns the variable name when `s` is exactly a builder variable reference. */
export function variableName(s: string | undefined): string | undefined {
  const m = VARIABLE_RE.exec((s ?? '').trim());
  return m ? (m[1] ?? m[2]) : undefined;
}

/** A number literal as typed: decimal or exponent form, finite, never via JS Number arithmetic. */
const NUMBER_RE = /^-?(\d+\.?\d*|\.\d+)([eE][+-]?\d{1,3})?$/;

export function isNumberLexeme(s: string): boolean {
  return NUMBER_RE.test(s) && Number.isFinite(Number(s));
}

/** Double-quoted SQL identifier. */
export function quoteIdent(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"';
}

/** Single-quoted SQL string literal. */
export function quoteString(value: string): string {
  return "'" + value.replace(/'/g, "''") + "'";
}

/** Intervals `$__timeGroup` accepts, as the backend parses them (query.go parseIntervalGrammar). */
const INTERVAL_RE =
  /^(\d+)\s*(ms|msecs?|milliseconds?|s|secs?|seconds?|m|mins?|minutes?|h|hrs?|hours?|d|days?|w|weeks?)$/i;

export function isValidInterval(s: string): boolean {
  return s === '$__interval' || INTERVAL_RE.test(s.trim());
}

/** Aggregations that read a numeric column. */
export const NUMERIC_ONLY: ReadonlySet<AggregateFn> = new Set([
  'mean',
  'median',
  'sum',
  'stddev',
  'spread',
  'percentile',
]);

export function isAggregate(fn: AggregateFn): boolean {
  return fn !== 'raw';
}

/** The output column name a select row gets when it has no alias of its own. */
export function defaultAlias(item: SelectItem): string {
  switch (item.fn) {
    case 'raw':
      return item.column;
    case 'count_rows':
      return 'count';
    case 'percentile':
      return 'p' + (item.param ?? '');
    default:
      return item.fn;
  }
}

/** SQL for one select row's expression (no alias). */
function selectExpr(item: SelectItem): string {
  const c = quoteIdent(item.column);
  switch (item.fn) {
    case 'raw':
      return c;
    case 'mean':
      return `avg(${c})`;
    case 'median':
      return `median(${c})`;
    case 'sum':
      return `sum(${c})`;
    case 'count':
      return `count(${c})`;
    case 'count_rows':
      return 'count(*)';
    case 'count_distinct':
      return `count(DISTINCT ${c})`;
    case 'min':
      return `min(${c})`;
    case 'max':
      return `max(${c})`;
    // InfluxQL first/last mean earliest/latest by time. DuckDB's first() is
    // order-dependent, arg_min/arg_max are not, and both skip NULL values.
    case 'first':
      return `arg_min(${c}, time)`;
    case 'last':
      return `arg_max(${c}, time)`;
    case 'stddev':
      return `stddev_samp(${c})`;
    case 'spread':
      return `max(${c}) - min(${c})`;
    case 'percentile':
      // Emitted as typed (99.9 / 100.0): no JS float arithmetic, so 99.9 is
      // not 0.9990000000000001.
      return `quantile_cont(${c}, ${item.param} / 100.0)`;
  }
}

/** Strips InfluxQL-style /…/ delimiters from a regex. */
export function stripRegexSlashes(p: string): string {
  return p.length >= 2 && p.startsWith('/') && p.endsWith('/') ? p.slice(1, -1) : p;
}

function literal(f: Filter): string {
  const v = f.value ?? '';
  switch (f.valueType) {
    case 'number':
      return v.trim();
    case 'boolean':
      return v.trim().toLowerCase();
    default:
      return quoteString(f.op === '=~' || f.op === '!~' ? stripRegexSlashes(v) : v);
  }
}

/**
 * One filter as SQL. Negated operators keep rows where the column is NULL:
 * InfluxQL's `!=` matches series that lack the tag. (`IS DISTINCT FROM` would
 * say this more briefly, but Arc refuses `FROM '<literal>'` as a table
 * position, on 26.09.3 and main alike.)
 */
function filterSQL(f: Filter): string {
  const c = quoteIdent(f.column);
  if (f.op === 'IS NULL' || f.op === 'IS NOT NULL') {
    return `${c} ${f.op}`;
  }
  const varName = variableName(f.value);
  if (varName !== undefined) {
    const ref = '$' + varName;
    switch (f.op) {
      case '=':
        return `${c} IN (${ref})`;
      case '!=':
        // An empty selection interpolates to NULL: `x NOT IN (NULL)` is NULL,
        // which coalesce turns into "exclude nothing". A NULL x is kept too.
        return `coalesce(${c} NOT IN (${ref}), true)`;
      case '=~':
        return `regexp_matches(${c}, ${ref})`;
      case '!~':
        return `(NOT regexp_matches(${c}, ${ref}) OR ${c} IS NULL)`;
      case 'NOT LIKE':
        return `(${c} NOT LIKE ${ref} OR ${c} IS NULL)`;
      default:
        return `${c} ${f.op} ${ref}`;
    }
  }
  const lit = literal(f);
  switch (f.op) {
    case '!=':
      return `(${c} != ${lit} OR ${c} IS NULL)`;
    case '=~':
      return `regexp_matches(${c}, ${lit})`;
    case '!~':
      return `(NOT regexp_matches(${c}, ${lit}) OR ${c} IS NULL)`;
    case 'NOT LIKE':
      return `(${c} NOT LIKE ${lit} OR ${c} IS NULL)`;
    default:
      return `${c} ${f.op} ${lit}`;
  }
}

const VALUE_OPS: ReadonlySet<FilterOp> = new Set<FilterOp>([
  '=',
  '!=',
  '<',
  '<=',
  '>',
  '>=',
  '=~',
  '!~',
  'LIKE',
  'NOT LIKE',
]);

function validateFilter(f: Filter, i: number, errors: FieldError[]) {
  const at = `where.${i}`;
  if (!f.column) {
    errors.push({ field: `${at}.column`, message: 'Pick a column' });
  }
  if (!VALUE_OPS.has(f.op)) {
    return;
  }
  const v = (f.value ?? '').trim();
  if (variableName(f.value) !== undefined) {
    return;
  }
  if (v.startsWith('$')) {
    errors.push({ field: `${at}.value`, message: 'Use a variable as $name or ${name}, without a :format' });
    return;
  }
  if (f.valueType === 'number') {
    if (!isNumberLexeme(v)) {
      errors.push({ field: `${at}.value`, message: `"${f.value ?? ''}" is not a number` });
    }
  } else if (f.valueType === 'boolean') {
    if (v.toLowerCase() !== 'true' && v.toLowerCase() !== 'false') {
      errors.push({ field: `${at}.value`, message: 'Use true or false' });
    }
  } else if (f.value === undefined) {
    errors.push({ field: `${at}.value`, message: 'Enter a value' });
  }
}

/**
 * Generates the SQL for a builder state, or explains why it cannot.
 * Never throws; never consults the schema.
 */
export function generate(b: BuilderQuery, format: Format): GenerateResult {
  const errors: FieldError[] = [];

  const m = (b.measurement ?? '').trim();
  const measurementVar = variableName(m);
  if (!m) {
    errors.push({ field: 'measurement', message: 'Pick a measurement' });
  } else if (measurementVar === undefined && !MEASUREMENT_RE.test(m)) {
    errors.push({ field: 'measurement', message: `"${m}" is not a measurement name Arc accepts` });
  }

  if (b.select.length === 0) {
    errors.push({ field: 'select', message: 'Add a field to SELECT' });
  }
  const aggregated = b.select.some((s) => isAggregate(s.fn));
  const raw = b.select.some((s) => !isAggregate(s.fn));
  if (aggregated && raw) {
    errors.push({ field: 'select', message: 'Mix of raw fields and aggregations: aggregate every field, or none' });
  }

  const tags = b.groupBy.tags;
  const reserved = new Set(['time', ...tags.map((t) => t.toLowerCase())]);
  const seen = new Set<string>();
  b.select.forEach((s, i) => {
    if (s.fn !== 'count_rows' && !s.column) {
      errors.push({ field: `select.${i}.column`, message: 'Pick a column' });
    }
    if (s.fn === 'percentile') {
      const n = (s.param ?? '').trim();
      if (!/^\d+(\.\d+)?$/.test(n) || Number(n) > 100) {
        errors.push({ field: `select.${i}.param`, message: 'Percentile must be a number from 0 to 100' });
      }
    }
    const alias = (s.alias ?? '').trim() || defaultAlias(s);
    const key = alias.toLowerCase();
    // A raw time series selects tags as plain columns, so a raw field named
    // like a group-by tag is the tag itself, not a collision.
    if (reserved.has(key) && !(s.fn === 'raw' && tags.includes(s.column))) {
      errors.push({
        field: `select.${i}.alias`,
        message: `"${alias}" is used by ${key === 'time' ? 'the time column' : 'a GROUP BY tag'}; give this field an alias`,
      });
    } else if (seen.has(key)) {
      errors.push({ field: `select.${i}.alias`, message: `Two fields are named "${alias}"; give one an alias` });
    }
    seen.add(key);
  });

  b.where.forEach((f, i) => validateFilter(f, i, errors));
  tags.forEach((t, i) => {
    if (!t) {
      errors.push({ field: `groupBy.tags.${i}`, message: 'Pick a tag' });
    }
  });

  const timeGroup = b.groupBy.time;
  if (timeGroup && !isValidInterval(timeGroup.interval)) {
    errors.push({
      field: 'groupBy.time',
      message: `"${timeGroup.interval}" is not an interval (use e.g. 10s, 5m, 1h, 1d)`,
    });
  }
  if (aggregated && format === 'time_series' && !timeGroup) {
    errors.push({ field: 'groupBy.time', message: 'An aggregated time series needs GROUP BY time' });
  }
  const limit = (b.limit ?? '').trim();
  if (limit && !/^[1-9]\d{0,9}$/.test(limit)) {
    errors.push({ field: 'limit', message: 'LIMIT must be a positive whole number' });
  }
  if (errors.length > 0) {
    return { errors };
  }

  const from = measurementVar !== undefined ? quoteIdent('${' + measurementVar + ':raw}') : quoteIdent(m);
  const where = ['$__timeFilter(time)', ...b.where.map(filterSQL)].join(' AND ');
  const tagCols = tags.map(quoteIdent);
  const fields = b.select.map((s) => {
    const alias = (s.alias ?? '').trim();
    if (s.fn === 'raw' && !alias) {
      return quoteIdent(s.column);
    }
    return `${selectExpr(s)} AS ${quoteIdent(alias || defaultAlias(s))}`;
  });
  const limitSQL = limit ? `\nLIMIT ${limit}` : '';

  if (!aggregated) {
    // Raw rows: tags first so a time series splits into one series per tag set.
    const rawFields = fields.filter(
      (_, i) => !tags.includes(b.select[i].column) || (b.select[i].alias ?? '').trim() !== ''
    );
    const cols = ['time', ...tagCols, ...rawFields].join(', ');
    const order = format === 'time_series' ? 'ASC' : (b.orderByTime ?? 'DESC');
    return { sql: `SELECT ${cols}\nFROM ${from}\nWHERE ${where}\nORDER BY time ${order}${limitSQL}` };
  }

  if (timeGroup) {
    const interval = timeGroup.interval === '$__interval' ? '$__interval' : quoteString(timeGroup.interval.trim());
    const cols = [`$__timeGroup(time, ${interval}) AS "time"`, ...tagCols, ...fields].join(', ');
    const groupBy = Array.from({ length: 1 + tagCols.length }, (_, i) => String(i + 1)).join(', ');
    const order = format === 'time_series' ? 'ASC' : (b.orderByTime ?? 'ASC');
    return {
      sql: `SELECT ${cols}\nFROM ${from}\nWHERE ${where}\nGROUP BY ${groupBy}\nORDER BY 1${order === 'DESC' ? ' DESC' : ''}${limitSQL}`,
    };
  }

  // Table, aggregated, no time group: one row per tag set (or one row). No
  // ORDER BY time: time is not in the output.
  const cols = [...tagCols, ...fields].join(', ');
  const groupBy = tagCols.length ? `\nGROUP BY ${tagCols.map((_, i) => String(i + 1)).join(', ')}` : '';
  return { sql: `SELECT ${cols}\nFROM ${from}\nWHERE ${where}${groupBy}${limitSQL}` };
}

/**
 * SQL listing the distinct values of `column`, for a filter's value
 * dropdown. The other filters apply too, rendered by the same rules, so the
 * suggestions are the values that can still match. Never more than limit+1
 * rows: the extra one says "there are more".
 */
export function valuesSQL(
  b: BuilderQuery,
  column: string,
  otherFilters: Filter[],
  search: string,
  limit: number
): string | undefined {
  const m = (b.measurement ?? '').trim();
  const measurementVar = variableName(m);
  if (!m || (measurementVar === undefined && !MEASUREMENT_RE.test(m)) || !column) {
    return undefined;
  }
  const errors: FieldError[] = [];
  otherFilters.forEach((f, i) => validateFilter(f, i, errors));
  const usable = errors.length === 0 ? otherFilters : [];
  const c = quoteIdent(column);
  const clauses = ['$__timeFilter(time)', ...usable.map(filterSQL)];
  const q = search.trim().slice(0, 100);
  if (q) {
    // contains(), not LIKE: no wildcard escaping to get wrong.
    clauses.push(`contains(lower(CAST(${c} AS VARCHAR)), lower(${quoteString(q)}))`);
  }
  const from = measurementVar !== undefined ? quoteIdent('${' + measurementVar + ':raw}') : quoteIdent(m);
  return `SELECT DISTINCT ${c} AS "value"\nFROM ${from}\nWHERE ${clauses.join(' AND ')}\nORDER BY 1\nLIMIT ${limit + 1}`;
}
