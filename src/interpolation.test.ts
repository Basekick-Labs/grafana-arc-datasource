import { escapeLiteral, isBuiltInVariable, resolveTimezone } from './interpolation';

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

describe('escapeLiteral', () => {
  // Regression: 1.3.2 changed single-value variables to add surrounding
  // quotes. Dashboards write `WHERE host = '$server'` with the quotes in the
  // SQL, so the value arrived as ''h01'' and every such panel failed with
  // "Parser Error: syntax error".
  it('does not add surrounding quotes', () => {
    expect(escapeLiteral('h01-customers-basekick-net')).toBe('h01-customers-basekick-net');
  });

  // R2-HI5: the payload must stay inside the author's literal. Doubling the
  // quote is what prevents it terminating the string and appending SQL.
  it('doubles embedded quotes so a payload cannot escape the literal', () => {
    expect(escapeLiteral("1' OR 1=1--")).toBe("1'' OR 1=1--");
    expect(`WHERE host = '${escapeLiteral("1' OR 1=1--")}'`).toBe("WHERE host = '1'' OR 1=1--'");
  });

  it('leaves a value with no quotes untouched', () => {
    expect(escapeLiteral('cpu-total')).toBe('cpu-total');
  });
});

// The two shapes the real "System Monitoring ARC v2" dashboard uses.
describe('dashboard idioms', () => {
  it("host = '$server' with a single-value variable stays a valid literal", () => {
    const sql = `WHERE host = '${escapeLiteral('h01-customers-basekick-net')}'`;
    expect(sql).toBe("WHERE host = 'h01-customers-basekick-net'");
  });

  it('a hostile value stays inside the literal', () => {
    const sql = `WHERE host = '${escapeLiteral("x' OR 1=1--")}'`;
    expect(sql).toBe("WHERE host = 'x'' OR 1=1--'");
    // One opening and one closing quote at the edges; the payload's quote is
    // doubled, so the literal is never terminated early.
    expect(sql.match(/'/g)!.length).toBe(4);
  });
});
