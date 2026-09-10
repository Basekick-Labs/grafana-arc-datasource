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
 *
 * Limitation: doubling is sufficient for DuckDB's standard string literals,
 * where a backslash is an ordinary character, but NOT inside an `E'...'`
 * escape-string literal, where `\'` consumes one quote and lets the next one
 * close the string. `WHERE host = E'$server'` is therefore unsafe with any
 * variable, and no idiom in this plugin's docs or tests uses that form.
 * Grafana's own Postgres datasource has the same gap. Use a standard literal.
 */
export function escapeLiteral(value: string): string {
  return value.replace(/'/g, "''");
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

/**
 * Normalises Grafana's dashboard timezone setting to an IANA zone name.
 *
 * Grafana reports the setting as an IANA name, the literal "utc", or
 * "browser" (meaning "whatever the viewer's browser is"). The backend only
 * understands IANA names, so "browser" is resolved HERE, where the browser
 * actually is — two viewers in different zones then each get their own local
 * bucketing, which a server-side default could not provide.
 *
 * Anything unset or unresolvable becomes "UTC", matching the behaviour the
 * plugin had before timezone support existed: a timezone lookup should never
 * black out a panel.
 */
export function resolveTimezone(timezone?: string): string {
  if (!timezone || timezone === 'utc' || timezone === 'UTC') {
    return 'UTC';
  }
  if (timezone === 'browser') {
    try {
      return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
    } catch {
      return 'UTC';
    }
  }
  return timezone;
}
