/**
 * Pure helpers for macro/variable interpolation, kept out of datasource.ts so
 * they can be unit-tested without pulling in @grafana/runtime.
 */

/**
 * Doubles embedded single quotes so a value cannot terminate the string
 * literal it is interpolated into, without adding surrounding quotes.
 *
 * This is the half of SQL-literal safety that actually stops injection. The
 * surrounding quotes belong to the query author (`WHERE host = '$server'`),
 * and adding a second pair here yields ''value'' -- a parser error, not extra
 * safety.
 */
export function escapeLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

/**
 * True for Grafana's built-in variables, whose names all start with `__`
 * (`$__interval`, `$__rate_interval`, `$__from`, `$__to`, `$__name`, ...).
 * Their values come from Grafana itself, not from the URL or a dashboard
 * user, so they must not be SQL-quoted.
 */
export function isBuiltInVariable(variable?: { name?: string }): boolean {
  return typeof variable?.name === 'string' && variable.name.startsWith('__');
}

/**
 * Normalises Grafana's dashboard timezone setting to an IANA zone name.
 *
 * "browser" (the default) is resolved via Intl to the viewer's actual zone;
 * "utc" and anything unset or unresolvable become "UTC". Returning UTC on
 * failure keeps behaviour identical to the pre-timezone plugin rather than
 * erroring a panel over a timezone lookup.
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
