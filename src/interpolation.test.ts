import { isBuiltInVariable, resolveTimezone } from './interpolation';

describe('isBuiltInVariable', () => {
  // Regression: Grafana routes its own $__interval through the datasource's
  // interpolation hook. Quoting it turned the documented
  // `time_bucket('$__interval', time)` into time_bucket(''30s'', time), and
  // every panel using that idiom failed with a parser error after 1.2.x.
  it('recognises built-in macros', () => {
    expect(isBuiltInVariable({ name: '__interval' })).toBe(true);
    expect(isBuiltInVariable({ name: '__rate_interval' })).toBe(true);
    expect(isBuiltInVariable({ name: '__from' })).toBe(true);
  });

  // These must still be quoted: their values can come from the URL.
  it('does not match user variables', () => {
    expect(isBuiltInVariable({ name: 'server' })).toBe(false);
    expect(isBuiltInVariable({ name: '_private' })).toBe(false);
    expect(isBuiltInVariable({})).toBe(false);
    expect(isBuiltInVariable(undefined)).toBe(false);
  });
});

describe('resolveTimezone', () => {
  it('normalises utc and empty to UTC', () => {
    expect(resolveTimezone('utc')).toBe('UTC');
    expect(resolveTimezone('UTC')).toBe('UTC');
    expect(resolveTimezone(undefined)).toBe('UTC');
    expect(resolveTimezone('')).toBe('UTC');
  });

  it('passes an IANA zone through', () => {
    expect(resolveTimezone('America/Costa_Rica')).toBe('America/Costa_Rica');
  });

  it('resolves browser to a concrete zone', () => {
    expect(resolveTimezone('browser')).not.toBe('browser');
  });
});
