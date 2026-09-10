/**
 * Pure helpers for template-variable interpolation, kept out of datasource.ts
 * so they can be unit-tested without pulling in @grafana/runtime.
 */

/**
 * Doubles embedded single quotes so a value cannot terminate the string
 * literal it is interpolated into, WITHOUT adding surrounding quotes.
 *
 * This is the half of SQL-literal safety that actually stops injection: a
 * payload like `x' OR 1=1--` becomes `x'' OR 1=1--`, which stays inside the
 * literal the query author opened. The surrounding quotes belong to the
 * author (`WHERE host = '$server'`), and adding a second pair here produces
 * `''value''` — a parser error, not extra safety.
 */
export function escapeLiteral(value: string): string {
  return String(value).replace(/'/g, "''");
}

/**
 * Renders a value as a complete SQL string literal: escaped and wrapped.
 *
 * Used for multi-value variables, which land in positions where the author
 * cannot pre-quote each element (`WHERE host IN ($hosts)`).
 */
export function quoteLiteral(value: string): string {
  return "'" + escapeLiteral(value) + "'";
}
