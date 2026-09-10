import { escapeLiteral, quoteLiteral } from './interpolation';

/**
 * These tests pin the interpolation contract against the idioms the real
 * production dashboards use. The 1.3.2 regression (finding R2-HI5) changed
 * single-value variables to add surrounding quotes on the false premise that
 * Grafana's Postgres datasource does the same; upstream
 * `packages/grafana-sql/src/datasource/SqlDatasource.ts` quotes only when
 * `variable.multi || variable.includeAll`.
 */
describe('escapeLiteral', () => {
  it('does not add surrounding quotes', () => {
    // The query author writes the quotes: WHERE host = '$server'.
    // Adding a second pair yields ''h01'' and a parser error.
    expect(escapeLiteral('h01-customers-basekick-net')).toBe('h01-customers-basekick-net');
  });

  it('doubles embedded quotes so a payload cannot escape the literal', () => {
    // This is the half of the guard that actually prevents injection: the
    // payload's quote is doubled, so it can never terminate the literal.
    expect(escapeLiteral("1' OR 1=1--")).toBe("1'' OR 1=1--");
  });

  it('leaves a value with no quotes untouched', () => {
    expect(escapeLiteral('cpu-total')).toBe('cpu-total');
  });
});

describe('quoteLiteral', () => {
  it('wraps and escapes', () => {
    expect(quoteLiteral('h01')).toBe("'h01'");
    expect(quoteLiteral("O'Brien")).toBe("'O''Brien'");
  });
});

/**
 * The idioms taken from the production "System Monitoring ARC v2" dashboard.
 * Panel SQL was extracted from the Grafana database rather than written here,
 * so these are the shapes that actually have to work.
 */
describe('production dashboard idioms', () => {
  const SERVER = 'h01-customers-basekick-net';

  it("host = '$server' stays a valid literal", () => {
    expect(`WHERE host = '${escapeLiteral(SERVER)}'`).toBe(`WHERE host = '${SERVER}'`);
  });

  it("host ~ '^$server$' keeps the regex anchors adjacent to the value", () => {
    // The 1.3.2 rule produced '^'h01'$' here, which is a parser error.
    expect(`WHERE host ~ '^${escapeLiteral(SERVER)}$'`).toBe(`WHERE host ~ '^${SERVER}$'`);
  });

  it('a multi-value variable is quoted per element for IN (...)', () => {
    const values = ['cpu0', 'cpu1'];
    expect(values.map(quoteLiteral).join(',')).toBe("'cpu0','cpu1'");
  });

  it('a hostile value stays inside the literal', () => {
    const sql = `WHERE host = '${escapeLiteral("x' OR 1=1--")}'`;
    expect(sql).toBe("WHERE host = 'x'' OR 1=1--'");
    // Exactly one opening and one closing quote at the edges; the payload's
    // own quote is doubled, so the literal is never terminated early.
    expect(sql.match(/'/g)!.length).toBe(4);
  });
});
