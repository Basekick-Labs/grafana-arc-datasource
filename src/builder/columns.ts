/**
 * Column kinds and what the builder offers for each (issue #20).
 *
 * Arc has no tag/field marker at query time, so the builder goes by type:
 * text columns behave like InfluxQL tags (filter and group by them), numeric
 * ones like fields (aggregate them).
 */
import { AggregateFn, FilterOp, FilterValueType, TransformKind } from '../types';
import { NUMERIC_ONLY } from './generate';

export type ColumnKind = 'string' | 'number' | 'boolean' | 'other' | 'unknown';

/** Classifies a DuckDB type name as Arc reports it. */
export function kindOf(type: string | undefined): ColumnKind {
  if (!type) {
    return 'unknown';
  }
  const t = type.toUpperCase();
  if (t === 'VARCHAR' || t === 'TEXT' || t === 'STRING' || t.startsWith('VARCHAR(')) {
    return 'string';
  }
  if (t === 'BOOLEAN' || t === 'BOOL') {
    return 'boolean';
  }
  if (
    /^(U?(TINY|SMALL|BIG|HUGE)?INT(EGER)?|INT[1248]|DOUBLE|FLOAT[48]?|REAL|DECIMAL(\(.*\))?|NUMERIC(\(.*\))?)$/.test(t)
  ) {
    return 'number';
  }
  return 'other';
}

export function valueTypeFor(kind: ColumnKind): FilterValueType {
  return kind === 'number' ? 'number' : kind === 'boolean' ? 'boolean' : 'string';
}

const STRING_OPS: FilterOp[] = ['=', '!=', '=~', '!~', 'LIKE', 'NOT LIKE', 'IS NULL', 'IS NOT NULL'];
const NUMBER_OPS: FilterOp[] = ['=', '!=', '<', '<=', '>', '>=', 'IS NULL', 'IS NOT NULL'];
const BOOLEAN_OPS: FilterOp[] = ['=', '!=', 'IS NULL', 'IS NOT NULL'];

export function operatorsFor(kind: ColumnKind): FilterOp[] {
  switch (kind) {
    case 'number':
      return NUMBER_OPS;
    case 'boolean':
      return BOOLEAN_OPS;
    case 'string':
      return STRING_OPS;
    case 'unknown':
      return Array.from(new Set([...STRING_OPS, ...NUMBER_OPS]));
    default:
      return [];
  }
}

export interface FunctionOption {
  fn: AggregateFn;
  label: string;
  description: string;
}

export const FUNCTIONS: FunctionOption[] = [
  { fn: 'mean', label: 'mean', description: 'Average (avg)' },
  { fn: 'median', label: 'median', description: 'Median' },
  { fn: 'sum', label: 'sum', description: 'Sum' },
  { fn: 'count', label: 'count', description: 'Non-null values' },
  { fn: 'count_distinct', label: 'count distinct', description: 'Distinct values' },
  { fn: 'min', label: 'min', description: 'Smallest value' },
  { fn: 'max', label: 'max', description: 'Largest value' },
  { fn: 'first', label: 'first', description: 'Earliest value by time' },
  { fn: 'last', label: 'last', description: 'Latest value by time' },
  { fn: 'stddev', label: 'stddev', description: 'Sample standard deviation' },
  { fn: 'spread', label: 'spread', description: 'max - min' },
  { fn: 'percentile', label: 'percentile', description: 'Continuous percentile (0-100)' },
  { fn: 'raw', label: 'raw', description: 'No aggregation: every row' },
];

/**
 * Why `fn` cannot apply to a column of `kind` in `format`, or undefined when
 * it can. Unknown kinds (typed names, no schema) are allowed: Arc decides.
 */
export function functionIneligibility(
  fn: AggregateFn,
  kind: ColumnKind,
  format: 'time_series' | 'table'
): string | undefined {
  if (kind === 'unknown') {
    return undefined;
  }
  if (NUMERIC_ONLY.has(fn) && kind !== 'number') {
    return 'needs a numeric column';
  }
  // In a time series every text column becomes a series label, so a function
  // whose result has the column's own type must not return text there.
  if (format === 'time_series' && kind === 'string' && ['raw', 'min', 'max', 'first', 'last'].includes(fn)) {
    return 'returns text, which a time series treats as a label (use Table, or count)';
  }
  if (fn === 'raw' && format === 'time_series' && kind !== 'number') {
    return 'a raw time series needs numeric fields (use Table for text)';
  }
  if (kind === 'other' && fn !== 'count' && fn !== 'count_distinct' && fn !== 'raw') {
    return 'not available for this column type: use Code mode';
  }
  return undefined;
}

/** A sensible first function for a column, InfluxQL style. */
export function defaultFunctionFor(kind: ColumnKind): AggregateFn {
  return kind === 'number' || kind === 'unknown' ? 'mean' : 'count';
}

export interface TransformOption {
  kind: TransformKind;
  label: string;
  description: string;
}

export const TRANSFORMS: TransformOption[] = [
  {
    kind: 'non_negative_derivative',
    label: 'non_negative_derivative',
    description: 'Rate per unit, negative steps (counter resets) dropped',
  },
  { kind: 'derivative', label: 'derivative', description: 'Rate of change per unit' },
  {
    kind: 'non_negative_difference',
    label: 'non_negative_difference',
    description: 'Change from the previous value, negative ones dropped',
  },
  { kind: 'difference', label: 'difference', description: 'Change from the previous value' },
  { kind: 'moving_average', label: 'moving_average', description: 'Mean of the last N values' },
  { kind: 'cumulative_sum', label: 'cumulative_sum', description: 'Running total from the start of the range' },
];

/**
 * Why a transformation cannot follow `fn` over a column of `kind`, or
 * undefined when it can: transformations do arithmetic, so the value must be
 * a number. Counts always are; unknown kinds are allowed (Arc decides).
 */
export function transformIneligibility(fn: AggregateFn, kind: ColumnKind): string | undefined {
  if (fn === 'count' || fn === 'count_rows' || fn === 'count_distinct' || kind === 'number' || kind === 'unknown') {
    return undefined;
  }
  return 'needs a numeric value';
}
