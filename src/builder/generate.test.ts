import { BuilderQuery, Filter, emptyBuilder } from '../types';
import { defaultAlias, generate, isNumberLexeme, isValidInterval, valuesSQL, variableName } from './generate';

function b(patch: Partial<BuilderQuery>): BuilderQuery {
  return { ...emptyBuilder(), measurement: 'cpu', ...patch };
}

function sql(state: BuilderQuery, format: 'time_series' | 'table' = 'time_series'): string {
  const r = generate(state, format);
  if (r.sql === undefined) {
    throw new Error('expected SQL, got errors: ' + JSON.stringify(r.errors));
  }
  return r.sql;
}

function errors(state: BuilderQuery, format: 'time_series' | 'table' = 'time_series'): string[] {
  const r = generate(state, format);
  return (r.errors ?? []).map((e) => `${e.field}: ${e.message}`);
}

describe('generate: shapes (plan v3 §4.3)', () => {
  test('time series, aggregated, grouped by a tag', () => {
    expect(
      sql(
        b({
          select: [{ column: 'usage_idle', fn: 'mean' }],
          groupBy: { time: { interval: '$__interval' }, tags: ['host'] },
        })
      )
    ).toBe(
      'SELECT $__timeGroup(time, $__interval) AS "time", "host", avg("usage_idle") AS "mean"\n' +
        'FROM "cpu"\n' +
        'WHERE $__timeFilter(time)\n' +
        'GROUP BY 1, 2\n' +
        'ORDER BY 1'
    );
  });

  test('a fixed interval is quoted; several tags group positionally', () => {
    expect(
      sql(
        b({
          select: [{ column: 'usage_idle', fn: 'max' }],
          groupBy: { time: { interval: '5m' }, tags: ['host', 'region'] },
        })
      )
    ).toBe(
      'SELECT $__timeGroup(time, \'5m\') AS "time", "host", "region", max("usage_idle") AS "max"\n' +
        'FROM "cpu"\nWHERE $__timeFilter(time)\nGROUP BY 1, 2, 3\nORDER BY 1'
    );
  });

  test('time series, raw: tags first, time ascending, no GROUP BY', () => {
    expect(
      sql(
        b({
          select: [{ column: 'usage_idle', fn: 'raw' }],
          groupBy: { time: { interval: '$__interval' }, tags: ['host'] },
        })
      )
    ).toBe('SELECT time, "host", "usage_idle"\nFROM "cpu"\nWHERE $__timeFilter(time)\nORDER BY time ASC');
  });

  test('table, raw: newest first with the user limit', () => {
    expect(sql(b({ select: [{ column: 'note', fn: 'raw' }], limit: '1000' }), 'table')).toBe(
      'SELECT time, "note"\nFROM "cpu"\nWHERE $__timeFilter(time)\nORDER BY time DESC\nLIMIT 1000'
    );
  });

  test('table, aggregated, with a time group: ORDER BY 1, direction honored', () => {
    expect(
      sql(
        b({
          select: [{ column: 'v', fn: 'sum' }],
          orderByTime: 'DESC',
          groupBy: { time: { interval: '1h' }, tags: [] },
        }),
        'table'
      )
    ).toBe(
      'SELECT $__timeGroup(time, \'1h\') AS "time", sum("v") AS "sum"\nFROM "cpu"\nWHERE $__timeFilter(time)\nGROUP BY 1\nORDER BY 1 DESC'
    );
  });

  test('table, aggregated, no time group: no ORDER BY time (it is not in the output)', () => {
    expect(sql(b({ select: [{ column: 'v', fn: 'mean' }], groupBy: { tags: ['host'] } }), 'table')).toBe(
      'SELECT "host", avg("v") AS "mean"\nFROM "cpu"\nWHERE $__timeFilter(time)\nGROUP BY 1'
    );
    expect(sql(b({ select: [{ column: '', fn: 'count_rows' }], groupBy: { tags: [] } }), 'table')).toBe(
      'SELECT count(*) AS "count"\nFROM "cpu"\nWHERE $__timeFilter(time)'
    );
  });

  test('an aggregated time series needs GROUP BY time', () => {
    expect(errors(b({ select: [{ column: 'v', fn: 'mean' }], groupBy: { tags: [] } }))).toEqual([
      'groupBy.time: An aggregated time series needs GROUP BY time',
    ]);
  });

  test('a dashed measurement is quoted', () => {
    expect(sql(b({ measurement: 'disk-io', select: [{ column: 'v', fn: 'raw' }] }))).toContain('FROM "disk-io"');
  });
});

describe('generate: functions', () => {
  const fn = (s: BuilderQuery['select'][number]) =>
    sql(b({ select: [s], groupBy: { time: { interval: '1m' }, tags: [] } })).split('\n')[0];

  test.each([
    [{ column: 'v', fn: 'mean' }, 'avg("v") AS "mean"'],
    [{ column: 'v', fn: 'median' }, 'median("v") AS "median"'],
    [{ column: 'v', fn: 'sum' }, 'sum("v") AS "sum"'],
    [{ column: 'v', fn: 'count' }, 'count("v") AS "count"'],
    [{ column: '', fn: 'count_rows' }, 'count(*) AS "count"'],
    [{ column: 'host', fn: 'count_distinct' }, 'count(DISTINCT "host") AS "count_distinct"'],
    [{ column: 'v', fn: 'min' }, 'min("v") AS "min"'],
    [{ column: 'v', fn: 'max' }, 'max("v") AS "max"'],
    [{ column: 'v', fn: 'first' }, 'arg_min("v", time) AS "first"'],
    [{ column: 'v', fn: 'last' }, 'arg_max("v", time) AS "last"'],
    [{ column: 'v', fn: 'stddev' }, 'stddev_samp("v") AS "stddev"'],
    [{ column: 'v', fn: 'spread' }, 'max("v") - min("v") AS "spread"'],
    [{ column: 'v', fn: 'percentile', param: '99.9' }, 'quantile_cont("v", 99.9 / 100.0) AS "p99.9"'],
  ] as const)('%j', (item, expected) => {
    expect(fn(item as BuilderQuery['select'][number])).toContain(expected);
  });

  test('percentile must be a number from 0 to 100', () => {
    for (const p of ['', 'abc', '101', '-1', '1e2']) {
      expect(errors(b({ select: [{ column: 'v', fn: 'percentile', param: p }] }))).toContainEqual(
        'select.0.param: Percentile must be a number from 0 to 100'
      );
    }
  });

  test('raw and aggregated rows do not mix', () => {
    expect(
      errors(
        b({
          select: [
            { column: 'v', fn: 'raw' },
            { column: 'w', fn: 'mean' },
          ],
        })
      )[0]
    ).toMatch(/aggregate every field, or none/);
  });
});

describe('generate: aliases (review M1)', () => {
  test('defaults to the function name, InfluxQL style', () => {
    expect(defaultAlias({ column: 'v', fn: 'mean' })).toBe('mean');
    expect(defaultAlias({ column: 'v', fn: 'raw' })).toBe('v');
    expect(defaultAlias({ column: '', fn: 'count_rows' })).toBe('count');
  });

  test('two functions over one column must not collide', () => {
    expect(
      errors(
        b({
          select: [
            { column: 'v', fn: 'max' },
            { column: 'w', fn: 'max' },
          ],
        })
      )
    ).toContainEqual('select.1.alias: Two fields are named "max"; give one an alias');
    expect(
      sql(
        b({
          select: [
            { column: 'v', fn: 'max' },
            { column: 'w', fn: 'max', alias: 'max_w' },
          ],
        })
      )
    ).toContain('max("v") AS "max", max("w") AS "max_w"');
  });

  test('count rows next to a field named count collides', () => {
    expect(
      errors(
        b({
          select: [
            { column: '', fn: 'count_rows' },
            { column: 'count', fn: 'count' },
          ],
        })
      )[0]
    ).toMatch(/Two fields/);
  });

  test('time and group-by tags are reserved', () => {
    expect(errors(b({ select: [{ column: 'v', fn: 'mean', alias: 'time' }] }))[0]).toMatch(/time column/);
    expect(
      errors(
        b({
          select: [{ column: 'v', fn: 'mean', alias: 'Host' }],
          groupBy: { time: { interval: '1m' }, tags: ['host'] },
        })
      )[0]
    ).toMatch(/GROUP BY tag/);
  });

  test('a raw field that is also a group-by tag is the tag, not a collision', () => {
    expect(
      sql(
        b({
          select: [
            { column: 'host', fn: 'raw' },
            { column: 'v', fn: 'raw' },
          ],
          groupBy: { tags: ['host'] },
        }),
        'table'
      )
    ).toBe('SELECT time, "host", "v"\nFROM "cpu"\nWHERE $__timeFilter(time)\nORDER BY time DESC');
  });
});

describe('generate: filters and literals', () => {
  const where = (...filters: Filter[]) => {
    const s = sql(b({ select: [{ column: 'v', fn: 'raw' }], where: filters }));
    return s.split('\n')[2].replace('WHERE $__timeFilter(time) AND ', '');
  };

  test('strings: quotes doubled, unicode untouched', () => {
    expect(where({ column: 'host', op: '=', value: "o'neil", valueType: 'string' })).toBe(`"host" = 'o''neil'`);
    expect(where({ column: 'host', op: '=', value: 'ñandú "x"', valueType: 'string' })).toBe(`"host" = 'ñandú "x"'`);
    expect(where({ column: 'host', op: '=', value: '', valueType: 'string' })).toBe(`"host" = ''`);
  });

  test('numbers as typed: no float rounding, no quotes', () => {
    expect(where({ column: 'v', op: '>', value: '0.1', valueType: 'number' })).toBe('"v" > 0.1');
    expect(where({ column: 'v', op: '<=', value: '9007199254740993', valueType: 'number' })).toBe(
      '"v" <= 9007199254740993'
    );
    expect(where({ column: 'v', op: '>=', value: '-1.5e10', valueType: 'number' })).toBe('"v" >= -1.5e10');
  });

  test('numbers that are not numbers, or not finite, are refused', () => {
    for (const v of ['abc', '1,5', '1e400', '', '0x10', 'NaN', 'Infinity']) {
      expect(
        errors(
          b({ select: [{ column: 'v', fn: 'raw' }], where: [{ column: 'v', op: '=', value: v, valueType: 'number' }] })
        )
      ).toEqual([`where.0.value: "${v}" is not a number`]);
    }
    expect(isNumberLexeme('1e308')).toBe(true);
    expect(isNumberLexeme('1e309')).toBe(false);
  });

  test('booleans', () => {
    expect(where({ column: 'ok', op: '=', value: 'TRUE', valueType: 'boolean' })).toBe('"ok" = true');
    expect(
      errors(
        b({
          select: [{ column: 'v', fn: 'raw' }],
          where: [{ column: 'ok', op: '=', value: 'yes', valueType: 'boolean' }],
        })
      )
    ).toEqual(['where.0.value: Use true or false']);
  });

  test('negated operators keep rows where the column is NULL (InfluxQL parity; Arc refuses IS DISTINCT FROM)', () => {
    expect(where({ column: 'host', op: '!=', value: 'a', valueType: 'string' })).toBe(
      `("host" != 'a' OR "host" IS NULL)`
    );
    expect(where({ column: 'host', op: '!~', value: '^w', valueType: 'string' })).toBe(
      `(NOT regexp_matches("host", '^w') OR "host" IS NULL)`
    );
    expect(where({ column: 'host', op: 'NOT LIKE', value: 'w%', valueType: 'string' })).toBe(
      `("host" NOT LIKE 'w%' OR "host" IS NULL)`
    );
  });

  test('regex: InfluxQL /…/ delimiters are stripped', () => {
    expect(where({ column: 'host', op: '=~', value: '/^web.*/', valueType: 'string' })).toBe(
      `regexp_matches("host", '^web.*')`
    );
    expect(where({ column: 'host', op: '=~', value: '^db', valueType: 'string' })).toBe(
      `regexp_matches("host", '^db')`
    );
  });

  test('IS NULL takes no value', () => {
    expect(where({ column: 'host', op: 'IS NULL' })).toBe('"host" IS NULL');
    expect(where({ column: 'host', op: 'IS NOT NULL', value: 'ignored' })).toBe('"host" IS NOT NULL');
  });

  test('a missing value is an error', () => {
    expect(
      errors(b({ select: [{ column: 'v', fn: 'raw' }], where: [{ column: 'host', op: '=', valueType: 'string' }] }))
    ).toEqual(['where.0.value: Enter a value']);
  });

  test('identifiers with quotes and apostrophes are quoted, not refused', () => {
    expect(where({ column: `it's "x"`, op: '=', value: 'a', valueType: 'string' })).toBe(`"it's ""x""" = 'a'`);
  });
});

describe('generate: variables (plan v3 §4.4)', () => {
  const where = (f: Filter) =>
    sql(b({ select: [{ column: 'v', fn: 'raw' }], where: [f] }))
      .split('\n')[2]
      .replace('WHERE $__timeFilter(time) AND ', '');

  test('= and != become IN / NOT IN over a bare reference', () => {
    expect(where({ column: 'host', op: '=', value: '$host', valueType: 'string' })).toBe('"host" IN ($host)');
    expect(where({ column: 'host', op: '=', value: '${host}', valueType: 'string' })).toBe('"host" IN ($host)');
    expect(where({ column: 'host', op: '!=', value: '$host', valueType: 'string' })).toBe(
      'coalesce("host" NOT IN ($host), true)'
    );
  });

  test('other operators reference the variable bare', () => {
    expect(where({ column: 'v', op: '>', value: '$min', valueType: 'number' })).toBe('"v" > $min');
    expect(where({ column: 'host', op: '=~', value: '$re', valueType: 'string' })).toBe('regexp_matches("host", $re)');
  });

  test('a :format suffix or other $ forms are refused', () => {
    for (const v of ['${host:csv}', '$host.x', '$ host']) {
      expect(
        errors(
          b({
            select: [{ column: 'v', fn: 'raw' }],
            where: [{ column: 'host', op: '=', value: v, valueType: 'string' }],
          })
        )[0]
      ).toMatch(/without a :format/);
    }
  });

  test('a measurement variable is interpolated raw inside the quotes', () => {
    expect(sql(b({ measurement: '$m', select: [{ column: 'v', fn: 'raw' }] }))).toContain('FROM "${m:raw}"');
  });

  test('variableName', () => {
    expect(variableName('$host')).toBe('host');
    expect(variableName('${host}')).toBe('host');
    expect(variableName('$host ')).toBe('host');
    expect(variableName('$1host')).toBeUndefined();
    expect(variableName('host')).toBeUndefined();
  });
});

describe('generate: validation', () => {
  test('an empty builder explains what is missing', () => {
    expect(errors(emptyBuilder())).toEqual(['measurement: Pick a measurement', 'select: Add a field to SELECT']);
  });

  test('measurement names Arc refuses', () => {
    for (const m of ['a.b', '1cpu', 'x y', 'cpu;']) {
      expect(errors(b({ measurement: m, select: [{ column: 'v', fn: 'raw' }] }))[0]).toMatch(/not a measurement name/);
    }
  });

  test('intervals follow the backend grammar, ms included', () => {
    for (const i of ['$__interval', '1s', '500ms', '10 seconds', '5m', '1h', '1d', '2w']) {
      expect(isValidInterval(i)).toBe(true);
    }
    for (const i of ['1 month', '1y', 'abc', '5', '$__range']) {
      expect(isValidInterval(i)).toBe(false);
    }
  });

  test('LIMIT must be a positive whole number', () => {
    for (const l of ['0', '-1', '1.5', 'ten', '$limit']) {
      expect(errors(b({ select: [{ column: 'v', fn: 'raw' }], limit: l }))).toEqual([
        'limit: LIMIT must be a positive whole number',
      ]);
    }
  });

  test('generation never depends on the schema: the same state, the same SQL', () => {
    const s = b({ select: [{ column: 'gone', fn: 'mean' }], groupBy: { time: { interval: '1m' }, tags: [] } });
    expect(sql(s)).toBe(sql({ ...s }));
  });
});

describe('valuesSQL', () => {
  const state = b({
    where: [
      { column: 'region', op: '=', value: 'eu', valueType: 'string' },
      { column: 'host', op: '=', valueType: 'string' },
    ],
  });

  test('the other filters apply; capped at limit+1', () => {
    expect(valuesSQL(state, 'host', [state.where[0]], '', 100)).toBe(
      'SELECT DISTINCT "host" AS "value"\nFROM "cpu"\nWHERE $__timeFilter(time) AND "region" = \'eu\'\nORDER BY 1\nLIMIT 101'
    );
  });

  test('search uses contains(), not LIKE, so % and _ are literal', () => {
    expect(valuesSQL(state, 'host', [], "50%_o'k", 100)).toContain(
      `contains(lower(CAST("host" AS VARCHAR)), lower('50%_o''k'))`
    );
  });

  test('search is capped at 100 characters', () => {
    expect(valuesSQL(state, 'host', [], 'x'.repeat(500), 100)).toContain(`lower('${'x'.repeat(100)}')`);
  });

  test('an incomplete other filter is ignored rather than breaking the lookup', () => {
    expect(valuesSQL(state, 'region', [state.where[1]], '', 10)).toBe(
      'SELECT DISTINCT "region" AS "value"\nFROM "cpu"\nWHERE $__timeFilter(time)\nORDER BY 1\nLIMIT 11'
    );
  });

  test('no measurement, no lookup', () => {
    expect(valuesSQL(emptyBuilder(), 'host', [], '', 10)).toBeUndefined();
  });
});
