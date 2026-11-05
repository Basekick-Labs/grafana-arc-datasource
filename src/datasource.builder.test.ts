// A template service that applies the format the way Grafana dispatches it:
// an explicit ${name:raw} wins, otherwise the format function passed in.
// (builderFormat itself is proven against Grafana's real engine in
// builder/interpolate.test.ts; this file tests which format each mode uses.)
const vars: Record<string, string | string[]> = {
  host: ['a', "o'neil"],
  one: 'cpu',
  many: ['cpu', 'mem'],
  db: 'metrics',
  baddb: 'a b',
};
jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    constructor(public instanceSettings: unknown) {}
  },
  frameToMetricFindValue: () => [],
  getTemplateSrv: () => ({
    replace: (text: string, _scoped: unknown, format: (v: string | string[]) => string) =>
      text.replace(/\$\{(\w+)(?::(\w+))?\}|\$(\w+)/g, (match: string, a?: string, fmt?: string, b?: string) => {
        const name = (a ?? b) as string;
        if (!(name in vars)) {
          return match;
        }
        const v = vars[name];
        if (fmt === 'raw') {
          return Array.isArray(v) ? v.join(',') : v;
        }
        return String(format(v));
      }),
    getVariables: () => [],
  }),
}));

import { DataSourceInstanceSettings } from '@grafana/data';
import { ArcDataSource } from './datasource';
import { ArcDataSourceOptions, ArcQuery, emptyBuilder } from './types';
import { variableErrors } from './builder/variables';

function ds(jsonData: ArcDataSourceOptions = {}): ArcDataSource {
  return new ArcDataSource({ jsonData } as DataSourceInstanceSettings<ArcDataSourceOptions>);
}

const builderSQL = `SELECT time, "v"\nFROM "cpu"\nWHERE $__timeFilter(time) AND "host" IN ($host)\nORDER BY time ASC`;
function builderQuery(patch: Partial<ArcQuery> = {}): ArcQuery {
  return {
    refId: 'A',
    editorMode: 'builder',
    sql: builderSQL,
    builder: { ...emptyBuilder(), measurement: 'cpu', select: [{ column: 'v', fn: 'raw' }], sql: builderSQL },
    ...patch,
  };
}

describe('applyTemplateVariables', () => {
  test('builder queries quote every value; the builder state is not sent', () => {
    const out = ds().applyTemplateVariables(builderQuery(), {});
    expect(out.sql).toContain(`"host" IN ('a','o''neil')`);
    expect(out.builder).toBeUndefined();
  });

  test('Code queries keep the historical rule (single values bare, author quotes them)', () => {
    const out = ds().applyTemplateVariables({ refId: 'A', sql: "WHERE host = '$one'" }, {});
    expect(out.sql).toBe("WHERE host = 'cpu'");
  });

  test('a variable database resolves to one validated name, in both modes', () => {
    expect(ds().applyTemplateVariables({ refId: 'A', sql: 'SELECT 1', database: '$db' }, {}).database).toBe('metrics');
    expect(ds().applyTemplateVariables(builderQuery({ database: '${db}' }), {}).database).toBe('metrics');
    expect(() => ds().applyTemplateVariables({ refId: 'A', sql: 'SELECT 1', database: '$many' }, {})).toThrow(
      /exactly one value/
    );
    expect(() => ds().applyTemplateVariables({ refId: 'A', sql: 'SELECT 1', database: '$baddb' }, {})).toThrow(
      /not a valid database name/
    );
  });

  test('a multi-value measurement variable is refused, naming the variable', () => {
    const q = builderQuery();
    q.builder!.measurement = '$many';
    expect(() => ds().applyTemplateVariables(q, {})).toThrow(/measurement variable \$many/);
  });

  test('a builder query whose SQL was edited elsewhere is interpolated as Code', () => {
    const q = builderQuery({ sql: "SELECT 1 WHERE host = '$one'" });
    expect(ds().applyTemplateVariables(q, {}).sql).toBe("SELECT 1 WHERE host = 'cpu'");
  });
});

describe('interpolateVariablesInQueries (new alert rule from a panel)', () => {
  test('stores today’s values as Code: alert evaluation never interpolates', () => {
    const [out] = ds().interpolateVariablesInQueries([builderQuery({ database: '$db' })], {});
    expect(out.editorMode).toBe('code');
    expect(out.builder).toBeUndefined();
    expect(out.sql).toContain(`IN ('a','o''neil')`);
    expect(out.database).toBe('metrics');
  });
});

describe('filterQuery', () => {
  test('an incomplete builder query is skipped', () => {
    expect(ds().filterQuery({ refId: 'A', editorMode: 'builder', sql: '', builder: emptyBuilder() })).toBe(false);
  });

  test('complete builder queries, Code queries and rawSql migrations run', () => {
    expect(ds().filterQuery(builderQuery())).toBe(true);
    expect(ds().filterQuery({ refId: 'A', sql: 'SELECT 1' })).toBe(true);
    expect(
      ds().filterQuery({ refId: 'A', editorMode: 'builder', builder: emptyBuilder(), sql: '', rawSql: 'SELECT 1' })
    ).toBe(true);
  });
});

describe('getDefaultQuery and settings', () => {
  test('new queries open in the builder, each with its own state', () => {
    const a = ds().getDefaultQuery('panel-editor' as never);
    const b = ds().getDefaultQuery('panel-editor' as never);
    expect(a.editorMode).toBe('builder');
    expect(a.builder).not.toBe(b.builder);
  });

  test('database override and default database come from the settings', () => {
    expect(ds().allowsDatabaseOverride()).toBe(true);
    expect(ds({ allowDatabaseOverride: false }).allowsDatabaseOverride()).toBe(false);
    expect(ds().defaultDatabase()).toBe('default');
    expect(ds({ database: 'metrics' }).defaultDatabase()).toBe('metrics');
  });
});

describe('variableErrors (edit-time guards)', () => {
  const b = { ...emptyBuilder(), measurement: 'cpu', where: [{ column: 'host', op: '=' as const, value: '$host' }] };

  test('multi-value variables only with = and !=', () => {
    const vs = [{ name: 'host', multi: true }];
    expect(variableErrors(b, undefined, vs, false)).toEqual([]);
    const like = { ...b, where: [{ column: 'host', op: 'LIKE' as const, value: '$host' }] };
    expect(variableErrors(like, undefined, vs, false)[0].message).toMatch(/several values/);
  });

  test('a custom All value is refused in filters', () => {
    expect(
      variableErrors(b, undefined, [{ name: 'host', includeAll: true, allValue: '.*' }], false)[0].message
    ).toMatch(/custom "All"/);
  });

  test('database and measurement variables must be single-value', () => {
    const vs = [
      { name: 'm', multi: true },
      { name: 'd', includeAll: true },
    ];
    const errs = variableErrors({ ...b, where: [], measurement: '$m' }, '$d', vs, false).map((e) => e.field);
    expect(errs).toEqual(['database', 'measurement']);
  });

  test('unknown variables are flagged; Grafana built-ins are not', () => {
    expect(variableErrors(b, undefined, [], false)[0].message).toMatch(/No variable named \$host/);
    const builtin = { ...b, where: [{ column: 'v', op: '>' as const, value: '$__interval_ms' }] };
    expect(variableErrors(builtin, undefined, [], false)).toEqual([]);
  });

  test('alert rules cannot use dashboard variables at all', () => {
    expect(variableErrors(b, undefined, [{ name: 'host' }], true)[0].message).toMatch(/Alert rules/);
  });
});
