/**
 * Pure helpers for macro/variable interpolation, kept out of datasource.ts so
 * they can be unit-tested without pulling in @grafana/runtime.
 */

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
