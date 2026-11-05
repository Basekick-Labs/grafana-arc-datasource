/**
 * Template-variable formatting for builder queries (issue #20, plan v3 §4.4).
 *
 * Builder SQL writes variables bare (`"host" IN ($host)`), so this format
 * function always produces complete literals. Grafana's `:sqlstring` is not
 * used: it escapes `"` as `\"` (DuckDB has no backslash escapes, so a value
 * with a double quote would silently match nothing) and turns an empty
 * selection into `IN ()`, a parser error.
 */
import { quoteLiteral } from '../interpolation';

/**
 * Formats a variable's value for builder SQL: every element a quoted
 * literal, comma-joined; an empty selection is NULL, so `IN (NULL)` matches
 * nothing and `coalesce(x NOT IN (NULL), true)` excludes nothing. Grafana's
 * built-in `$__interval` comes through here too, quoted, which is the form
 * `$__timeGroup` takes.
 */
export function builderFormat(value: string | string[] | number): string {
  if (Array.isArray(value)) {
    return value.length === 0 ? 'NULL' : value.map((v) => quoteLiteral(String(v))).join(',');
  }
  if (typeof value === 'number') {
    return String(value);
  }
  return quoteLiteral(value);
}

/** Thrown for a variable that cannot stand where the builder put it. */
export class VariableUseError extends Error {}

/**
 * Formats a variable's value where only one plain name fits: a database or
 * measurement. A multi-value selection is an error, not a comma-joined name.
 */
export function rawSingle(value: string | string[] | number): string {
  if (Array.isArray(value)) {
    if (value.length !== 1) {
      throw new VariableUseError('a variable used as a database or measurement must have exactly one value selected');
    }
    return String(value[0]);
  }
  return String(value);
}
