import { functionIneligibility, kindOf, operatorsFor } from './columns';

test('kindOf classifies DuckDB type names as Arc reports them', () => {
  expect(kindOf('VARCHAR')).toBe('string');
  expect(kindOf('BOOLEAN')).toBe('boolean');
  for (const t of ['BIGINT', 'INTEGER', 'DOUBLE', 'FLOAT', 'DECIMAL(18,3)', 'HUGEINT', 'UBIGINT', 'REAL']) {
    expect(kindOf(t)).toBe('number');
  }
  for (const t of ['TIMESTAMP WITH TIME ZONE', 'DATE', 'BLOB', 'VARCHAR[]', 'STRUCT(a INTEGER)']) {
    expect(kindOf(t)).toBe('other');
  }
  expect(kindOf(undefined)).toBe('unknown');
});

test('operators follow the column type', () => {
  expect(operatorsFor('number')).toContain('>=');
  expect(operatorsFor('number')).not.toContain('=~');
  expect(operatorsFor('string')).toContain('=~');
  expect(operatorsFor('string')).not.toContain('<');
  expect(operatorsFor('boolean')).toEqual(['=', '!=', 'IS NULL', 'IS NOT NULL']);
  expect(operatorsFor('other')).toEqual([]);
});

test('function eligibility', () => {
  expect(functionIneligibility('mean', 'number', 'time_series')).toBeUndefined();
  expect(functionIneligibility('mean', 'string', 'table')).toMatch(/numeric/);
  expect(functionIneligibility('count', 'string', 'time_series')).toBeUndefined();
  expect(functionIneligibility('count_distinct', 'other', 'table')).toBeUndefined();
  expect(functionIneligibility('max', 'other', 'table')).toMatch(/Code mode/);
  expect(functionIneligibility('mean', 'unknown', 'time_series')).toBeUndefined();
});

test('a time series never gets a text-valued field (it would become a label)', () => {
  for (const fn of ['raw', 'min', 'max', 'first', 'last'] as const) {
    expect(functionIneligibility(fn, 'string', 'time_series')).toMatch(/label/);
    expect(functionIneligibility(fn, 'string', 'table')).toBeUndefined();
  }
  expect(functionIneligibility('last', 'number', 'time_series')).toBeUndefined();
});
