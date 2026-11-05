/**
 * The query builder's SQL generator (issue #20, plan v3 §4.3).
 *
 * Pure: state in, SQL or errors out. It never looks at the schema, so the
 * same state always yields the same SQL. A column that disappeared is a
 * warning in the editor, not a different query.
 */
import { AggregateFn, BuilderQuery, Filter, FilterOp, SelectItem, Transform } from '../types';

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

const FILL_RE = /^(null|previous|linear)$/;

/**
 * The WHERE clause: the time filter, then the user's filters. AND binds
 * tighter than OR, as in InfluxQL and SQL; with any OR, each AND-group is
 * parenthesised and the whole predicate wrapped, so an OR can never escape
 * the time filter.
 */
export function whereSQL(filters: Filter[]): string {
  const groups = andGroups(filters).map((g) => g.map((i) => filterSQL(filters[i])));
  if (groups.length === 0) {
    return '$__timeFilter(time)';
  }
  if (groups.length === 1) {
    return ['$__timeFilter(time)', ...groups[0]].join(' AND ');
  }
  const ors = groups.map((g) => (g.length > 1 ? `(${g.join(' AND ')})` : g[0]));
  return `$__timeFilter(time) AND (${ors.join(' OR ')})`;
}

/** Filter indexes by AND-group: a filter joined by OR starts a new group. */
export function andGroups(filters: Filter[]): number[][] {
  const groups: number[][] = [];
  filters.forEach((f, i) => {
    if (i === 0 || f.connector === 'OR') {
      groups.push([i]);
    } else {
      groups[groups.length - 1].push(i);
    }
  });
  return groups;
}

/**
 * The filters a value lookup for filter i applies: the others in its own
 * AND-group. A filter in another OR branch does not narrow which values
 * this one can match.
 */
export function lookupFilters(filters: Filter[], i: number): Filter[] {
  const group = andGroups(filters).find((g) => g.includes(i)) ?? [];
  return group.filter((j) => j !== i).map((j) => ({ ...filters[j], connector: undefined }));
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
    if (s.transform) {
      validateTransform(s, i, errors);
    }
    if (s.math !== undefined) {
      const m = parseMath(s.math);
      if ('error' in m) {
        errors.push({ field: `select.${i}.math`, message: m.error });
      }
    }
  });
  const names = outputNames(b.select, tags, errors);

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
  const fill = timeGroup?.fill;
  if (fill !== undefined && !FILL_RE.test(fill) && !isNumberLexeme(fill)) {
    errors.push({
      field: 'groupBy.time.fill',
      message: `"${fill}" is not a fill: use null, previous, linear or a number`,
    });
  }
  if (fill !== undefined && fill !== 'null' && b.select.some((s) => s.transform)) {
    errors.push({
      field: 'groupBy.time.fill',
      message: 'With a transformation, fill can only be null: filling before or after it changes the rates',
    });
  }
  if (aggregated && format === 'time_series' && !timeGroup) {
    errors.push({ field: 'groupBy.time', message: 'An aggregated time series needs GROUP BY time' });
  }
  const transformed = b.select.some((s) => s.transform);
  if (transformed && (aggregated ? !timeGroup : format === 'table')) {
    errors.push({
      field: 'select',
      message: 'Transformations need values in time order: use GROUP BY time, or a raw time series',
    });
  }
  tags.forEach((t, i) => {
    if (transformed && t.startsWith('__')) {
      errors.push({ field: `groupBy.tags.${i}`, message: `A tag named "${t}" cannot be used with transformations` });
    }
  });
  const limit = (b.limit ?? '').trim();
  if (limit && !/^[1-9]\d{0,9}$/.test(limit)) {
    errors.push({ field: 'limit', message: 'LIMIT must be a positive whole number' });
  }
  if (errors.length > 0) {
    return { errors };
  }

  const from = measurementVar !== undefined ? quoteIdent('${' + measurementVar + ':raw}') : quoteIdent(m);
  const where = whereSQL(b.where);
  const tagCols = tags.map(quoteIdent);
  const limitSQL = limit ? `\nLIMIT ${limit}` : '';
  const interval = timeGroup
    ? timeGroup.interval === '$__interval'
      ? '$__interval'
      : quoteString(timeGroup.interval.trim())
    : '';

  // Fill is the backend's: it fills time series only, so a table's SQL omits it.
  const fillArg = fill !== undefined && format === 'time_series' ? `, ${fill === 'null' ? 'NULL' : fill}` : '';
  if (transformed) {
    return { sql: transformedSQL(b, format, names, from, where, tagCols, interval, limitSQL, fillArg) };
  }

  const fields = b.select.map((s, i) => {
    const expr = applyMath(selectExpr(s), s.math);
    // A raw column keeps its own name unaliased: the SQL v1 always wrote.
    if (s.fn === 'raw' && !s.math && names[i] === s.column) {
      return quoteIdent(s.column);
    }
    return `${expr} AS ${quoteIdent(names[i])}`;
  });

  if (!aggregated) {
    // Raw rows: tags first so a time series splits into one series per tag set.
    const rawFields = fields.filter(
      (_, i) => !tags.includes(b.select[i].column) || (b.select[i].alias ?? '').trim() !== '' || !!b.select[i].math
    );
    const cols = ['time', ...tagCols, ...rawFields].join(', ');
    const order = format === 'time_series' ? 'ASC' : (b.orderByTime ?? 'DESC');
    return { sql: `SELECT ${cols}\nFROM ${from}\nWHERE ${where}\nORDER BY time ${order}${limitSQL}` };
  }

  if (timeGroup) {
    const cols = [`$__timeGroup(time, ${interval}${fillArg}) AS "time"`, ...tagCols, ...fields].join(', ');
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
 * Output column names, in select order. An explicit alias is used as typed
 * and must be unique; a default name (the function, or the transformation)
 * that is already taken gets `_1`, `_2`…, as InfluxQL names
 * `non_negative_derivative`, `non_negative_derivative_1`.
 */
function outputNames(select: SelectItem[], tags: string[], errors: FieldError[]): string[] {
  const reserved = new Set(['time', ...tags.map((t) => t.toLowerCase())]);
  const taken = new Set<string>();
  select.forEach((s, i) => {
    const alias = (s.alias ?? '').trim();
    if (!alias) {
      return;
    }
    const key = alias.toLowerCase();
    if (reserved.has(key)) {
      errors.push({
        field: `select.${i}.alias`,
        message: `"${alias}" is used by ${key === 'time' ? 'the time column' : 'a GROUP BY tag'}; give this field an alias`,
      });
    } else if (taken.has(key)) {
      errors.push({ field: `select.${i}.alias`, message: `Two fields are named "${alias}"; give one an alias` });
    }
    taken.add(key);
  });
  return select.map((s) => {
    const alias = (s.alias ?? '').trim();
    if (alias) {
      return alias;
    }
    const base = s.transform ? s.transform.kind : defaultAlias(s);
    // A raw time series selects tags as plain columns, so a raw field named
    // like a group-by tag is the tag itself, not a collision.
    if (s.fn === 'raw' && !s.transform && !s.math && tags.includes(s.column)) {
      return base;
    }
    let name = base;
    for (let k = 1; reserved.has(name.toLowerCase()) || taken.has(name.toLowerCase()); k++) {
      name = `${base}_${k}`;
    }
    taken.add(name.toLowerCase());
    return name;
  });
}

const TRANSFORM_KINDS: ReadonlySet<string> = new Set([
  'derivative',
  'non_negative_derivative',
  'difference',
  'non_negative_difference',
  'moving_average',
  'cumulative_sum',
]);

/** A derivative's unit: InfluxQL duration units, never rounded (query.go rounds ms for buckets). */
const UNIT_RE = /^(\d{1,6})(ns|us|µs|u|ms|s|m|h|d)$/;
const UNIT_NS: Record<string, number> = {
  ns: 1,
  us: 1e3,
  µs: 1e3,
  u: 1e3,
  ms: 1e6,
  s: 1e9,
  m: 60e9,
  h: 3600e9,
  d: 86400e9,
};

/** A unit in nanoseconds, or undefined when it is not one. */
export function unitNanos(unit: string): number | undefined {
  const m = UNIT_RE.exec(unit.trim());
  if (!m || Number(m[1]) === 0) {
    return undefined;
  }
  const ns = Number(m[1]) * UNIT_NS[m[2]];
  return Number.isSafeInteger(ns) ? ns : undefined;
}

function validateTransform(s: SelectItem, i: number, errors: FieldError[]) {
  const t = s.transform!;
  const at = `select.${i}.transform`;
  if (!TRANSFORM_KINDS.has(t.kind)) {
    errors.push({ field: at, message: `Unknown transformation "${t.kind}"` });
    return;
  }
  if ((t.kind === 'derivative' || t.kind === 'non_negative_derivative') && unitNanos(t.unit ?? '1s') === undefined) {
    errors.push({ field: `${at}.unit`, message: `"${t.unit}" is not a unit (use e.g. 1s, 10s, 1m, 500ms)` });
  }
  if (t.kind === 'moving_average') {
    const n = (t.n ?? '').trim();
    if (!/^\d{1,4}$/.test(n) || Number(n) < 2 || Number(n) > 1000) {
      errors.push({ field: `${at}.n`, message: 'moving_average needs a window of 2 to 1000 values' });
    }
  }
}

const MATH_TOKEN_RE = /^\s*([-+*/])\s*(-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d{1,3})?)/;

/**
 * Parses a math chain such as `* 8 / 1024`: up to five steps, each an
 * operator and a number. Applied left to right, so `+` and `-` cannot be
 * mixed with `*` and `/`: InfluxQL would apply precedence, and a reader
 * would expect it.
 */
export function parseMath(text: string): { steps: Array<{ op: string; value: string }> } | { error: string } {
  let rest = text.trim();
  const steps: Array<{ op: string; value: string }> = [];
  if (!rest) {
    return { steps };
  }
  while (rest) {
    const m = MATH_TOKEN_RE.exec(rest);
    if (!m || !isNumberLexeme(m[2])) {
      return { error: `Math "${text.trim()}" is not a chain like "* 8" or "/ 1024 / 1024"` };
    }
    if (m[1] === '/' && Number(m[2]) === 0) {
      return { error: 'Math divides by zero' };
    }
    steps.push({ op: m[1], value: m[2] });
    rest = rest.slice(m[0].length).trim();
  }
  if (steps.length > 5) {
    return { error: 'Math takes at most five steps' };
  }
  const additive = steps.some((s) => s.op === '+' || s.op === '-');
  const multiplicative = steps.some((s) => s.op === '*' || s.op === '/');
  if (additive && multiplicative) {
    return { error: 'Math mixes + or - with * or /: use Code mode, so the order is explicit' };
  }
  return { steps };
}

/** Wraps expr in the math chain, fully parenthesised, negatives too: `(x) - (-5)`, never `--5`. */
function applyMath(expr: string, math: string | undefined): string {
  if (!math) {
    return expr;
  }
  const m = parseMath(math);
  if ('error' in m) {
    return expr;
  }
  // The base is parenthesised too: spread and difference are `a - b`, and
  // `a - b * 8` would multiply b alone.
  return m.steps.reduce(
    (e, s) => `(${e} ${s.op} ${s.value.startsWith('-') ? `(${s.value})` : s.value})`,
    m.steps.length ? `(${expr})` : expr
  );
}

/** The CTE a transformed query buckets in. Improbable, so no measurement shares it (Arc treats a same-named table as the CTE). */
const CTE = 'arc_builder_buckets';
const RANKED = 'arc_builder_ranked';
const PRECEDING = 'ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING';

/** Buckets before the range a transformation reads, as InfluxQL does: one for a change, n-1 for a moving average. */
function lookback(t: Transform | undefined): number {
  switch (t?.kind) {
    case 'derivative':
    case 'non_negative_derivative':
    case 'difference':
    case 'non_negative_difference':
      return 1;
    case 'moving_average':
      return Number(t.n) - 1;
    default:
      return 0;
  }
}

/**
 * One transformation over inner column v (row i), or undefined for a row
 * without one. `start` is set when the inner query reads buckets before the
 * range: a running sum must not count them.
 */
function transformExpr(t: Transform | undefined, i: number, partition: string, start?: string): string | undefined {
  if (!t) {
    return undefined;
  }
  const v = `"__v${i}"`;
  // The previous non-NULL value and its time, so a NULL bucket is skipped
  // rather than breaking the chain (InfluxQL skips nulls too).
  const pv = `last_value(${v} IGNORE NULLS) OVER (w ${PRECEDING})`;
  const pt = `last_value(CASE WHEN ${v} IS NOT NULL THEN "__t" END IGNORE NULLS) OVER (w ${PRECEDING})`;
  const rate = () => `(${v} - ${pv}) / NULLIF(epoch_ns("__t") - epoch_ns(${pt}), 0) * ${unitNanos(t.unit ?? '1s')}`;
  switch (t.kind) {
    case 'derivative':
      return rate();
    case 'non_negative_derivative':
      // Time only moves forward here, so the rate is negative exactly when the value fell.
      return `CASE WHEN ${v} >= ${pv} THEN ${rate()} END`;
    case 'difference':
      return `${v} - ${pv}`;
    case 'non_negative_difference':
      return `CASE WHEN ${v} >= ${pv} THEN ${v} - ${pv} END`;
    case 'moving_average': {
      // The last n values that exist, as InfluxQL: "__k" counts the series'
      // non-NULL values so far, and a range frame over it reaches back n of
      // them, however many NULL buckets lie between.
      const n = Number(t.n);
      const k = `"__k${i}"`;
      return `CASE WHEN ${v} IS NOT NULL AND ${k} >= ${n} THEN avg(${v}) OVER (${partition}ORDER BY ${k} RANGE BETWEEN ${n - 1} PRECEDING AND CURRENT ROW) END`;
    }
    case 'cumulative_sum': {
      const counted = start ? `CASE WHEN "__t" >= ${start} THEN ${v} END` : v;
      return `CASE WHEN ${v} IS NOT NULL THEN sum(${counted}) OVER (w ROWS UNBOUNDED PRECEDING) END`;
    }
  }
}

/**
 * A query with transformations: the values are computed per bucket (or per
 * raw row) in a CTE, and the transformations run over them as window
 * functions, per series, in time order.
 *
 * - InfluxQL reads buckets before the range for a change or a moving
 *   average, so the first bucket has a value too: the CTE reads them
 *   ($__timeFilterLookback) and QUALIFY drops them again ($__timeGroupStart).
 * - LIMIT and ORDER BY apply to the outer query only, so the windows see
 *   every bucket.
 * - A row is dropped when every transformed value in it is NULL (each
 *   series' first, a counter reset), as InfluxQL drops those points; that
 *   also keeps an alert's Reduce from reading NaN.
 */
function transformedSQL(
  b: BuilderQuery,
  format: Format,
  names: string[],
  from: string,
  where: string,
  tagCols: string[],
  interval: string,
  limitSQL: string,
  fillArg: string
): string {
  const aggregated = b.select.some((s) => isAggregate(s.fn));
  const back = aggregated ? Math.max(...b.select.map((s) => lookback(s.transform))) : 0;
  const start = back > 0 ? `$__timeGroupStart(${interval})` : undefined;
  const innerWhere =
    back > 0 ? where.replace('$__timeFilter(time)', `$__timeFilterLookback(time, ${interval}, ${back})`) : where;
  const inner = b.select.map((s, i) => `${selectExpr(s)} AS "__v${i}"`);
  const head = aggregated ? `$__timeGroup(time, ${interval}${fillArg}) AS "__t"` : 'time AS "__t"';
  const groupBy = aggregated
    ? `\n  GROUP BY ${Array.from({ length: 1 + tagCols.length }, (_, i) => String(i + 1)).join(', ')}`
    : '';
  // Raw rows can share a timestamp; the values break the tie so the order is the same every run.
  const orderKeys = ['"__t"', ...(aggregated ? [] : b.select.map((_, i) => `"__v${i}"`))].join(', ');
  const partition = tagCols.length ? `PARTITION BY ${tagCols.join(', ')} ` : '';
  let ctes = `WITH ${CTE} AS (\n  SELECT ${[head, ...tagCols, ...inner].join(', ')}\n  FROM ${from}\n  WHERE ${innerWhere}${groupBy}\n)`;
  let source = CTE;
  const ranked = b.select.map((s, i) => (s.transform?.kind === 'moving_average' ? i : -1)).filter((i) => i >= 0);
  if (ranked.length) {
    const ks = ranked.map(
      (i) => `count("__v${i}") OVER (${partition}ORDER BY ${orderKeys} ROWS UNBOUNDED PRECEDING) AS "__k${i}"`
    );
    ctes += `,\n${RANKED} AS (\n  SELECT *, ${ks.join(', ')}\n  FROM ${CTE}\n)`;
    source = RANKED;
  }

  const transforms = b.select.map((s, i) => transformExpr(s.transform, i, partition, start));
  const outer = b.select.map((s, i) => `${applyMath(transforms[i] ?? `"__v${i}"`, s.math)} AS ${quoteIdent(names[i])}`);
  const window = `WINDOW w AS (${partition}ORDER BY ${orderKeys})`;
  const notNull = transforms
    .filter((t): t is string => t !== undefined)
    .map((t) => `(${t}) IS NOT NULL`)
    .join(' OR ');
  const qualify = start ? `QUALIFY "__t" >= ${start} AND (${notNull})` : `QUALIFY ${notNull}`;
  const order = format === 'time_series' ? 'ASC' : (b.orderByTime ?? 'ASC');
  return `${ctes}\nSELECT ${['"__t" AS "time"', ...tagCols, ...outer].join(', ')}\nFROM ${source}\n${window}\n${qualify}\nORDER BY 1${order === 'DESC' ? ' DESC' : ''}${limitSQL}`;
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
  const clauses = [whereSQL(usable)];
  const q = search.trim().slice(0, 100);
  if (q) {
    // contains(), not LIKE: no wildcard escaping to get wrong.
    clauses.push(`contains(lower(CAST(${c} AS VARCHAR)), lower(${quoteString(q)}))`);
  }
  const from = measurementVar !== undefined ? quoteIdent('${' + measurementVar + ':raw}') : quoteIdent(m);
  return `SELECT DISTINCT ${c} AS "value"\nFROM ${from}\nWHERE ${clauses.join(' AND ')}\nORDER BY 1\nLIMIT ${limit + 1}`;
}
