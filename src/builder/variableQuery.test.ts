import {
  VariableBuilder,
  emptyVariableBuilder,
  generateVariable,
  resolveVariableMode,
  toVariableQuery,
  variableDefinition,
} from './variableQuery';

const vb = (patch: Partial<VariableBuilder>): VariableBuilder => ({ ...emptyVariableBuilder(), ...patch });

describe('generateVariable', () => {
  test('databases and measurements store the SHOW statement an older plugin can still run', () => {
    expect(generateVariable(vb({ kind: 'databases' })).query).toBe('SHOW DATABASES');
    expect(generateVariable(vb({ kind: 'measurements' })).query).toBe('SHOW TABLES');
    expect(generateVariable(vb({ kind: 'measurements', database: 'telegraf' })).query).toBe(
      'SHOW TABLES FROM "telegraf"'
    );
  });

  test('tag values: distinct, non-NULL, in the time range, filters as in the panel builder', () => {
    expect(
      generateVariable(
        vb({
          measurement: 'cpu',
          column: 'host',
          where: [
            { column: 'region', op: '=', value: '$region', valueType: 'string' },
            { column: 'dc', op: '=', value: 'x', valueType: 'string', connector: 'OR' },
          ],
        })
      ).query
    ).toBe(
      `SELECT DISTINCT "host" AS "value"\nFROM "cpu"\nWHERE $__timeFilter(time) AND ("region" IN ($region) OR "dc" = 'x') AND "host" IS NOT NULL\nORDER BY 1\nLIMIT 1000`
    );
  });

  test('a measurement variable reads as one raw name', () => {
    expect(generateVariable(vb({ measurement: '$m', column: 'host' })).query).toContain('FROM "${m:raw}"');
  });

  test('tag keys have no SQL form', () => {
    expect(generateVariable(vb({ kind: 'columns', measurement: 'cpu' })).query).toBe(
      '-- Arc builder: text columns of cpu'
    );
  });

  test.each([
    [vb({ column: 'host' }), /Pick a measurement/],
    [vb({ measurement: 'cpu' }), /Pick a column/],
    [vb({ kind: 'columns' }), /Pick a measurement/],
    [vb({ measurement: 'cpu; drop', column: 'host' }), /not a measurement name/],
    [vb({ kind: 'measurements', database: 'a b' }), /not a database name/],
    [vb({ measurement: 'cpu', column: 'host', limit: '0' }), /LIMIT/],
  ])('refuses an incomplete or invalid state (%#)', (state, msg) => {
    expect((generateVariable(state).errors ?? []).map((e) => e.message).join()).toMatch(msg);
  });

  test('a readable definition for the variable list', () => {
    expect(variableDefinition(vb({ measurement: 'cpu', column: 'host' }))).toBe('Arc values of host in cpu');
    expect(variableDefinition(vb({ kind: 'columns', measurement: 'cpu' }))).toBe('Arc tag keys of cpu');
  });
});

describe('resolveVariableMode', () => {
  const built = (query: string, sql = query) => ({
    query,
    editorMode: 'builder' as const,
    builder: vb({ kind: 'databases', sql }),
  });

  test('a new, empty variable opens in the builder', () => {
    expect(resolveVariableMode(toVariableQuery(''))).toBe('builder');
    expect(resolveVariableMode(toVariableQuery({ query: '' }))).toBe('builder');
  });

  test('every variable saved before the builder opens in Code', () => {
    expect(resolveVariableMode(toVariableQuery('SELECT DISTINCT host FROM cpu'))).toBe('code');
    expect(resolveVariableMode(toVariableQuery({ query: 'SHOW DATABASES' }))).toBe('code');
    expect(resolveVariableMode(toVariableQuery({ rawSql: 'SELECT 1' }))).toBe('code');
  });

  test('the builder only while its SQL is the saved query', () => {
    expect(resolveVariableMode(built('SHOW DATABASES'))).toBe('builder');
    expect(resolveVariableMode(built('SHOW DATABASES -- edited', 'SHOW DATABASES'))).toBe('code');
    expect(resolveVariableMode({ ...built('SHOW DATABASES'), editorMode: 'code' })).toBe('code');
    expect(
      resolveVariableMode({ ...built('SHOW DATABASES'), builder: { ...vb({ sql: 'SHOW DATABASES' }), version: 2 } })
    ).toBe('code');
  });
});
