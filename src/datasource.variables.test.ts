// Grafana's frameToMetricFindValue takes the first text field; this stand-in
// does the same, without loading @grafana/ui under jsdom.
jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    constructor(public instanceSettings: unknown) {}
  },
  getTemplateSrv: () => ({ replace: (s: string) => s, getVariables: () => [] }),
  frameToMetricFindValue: (frame: { fields: Array<{ type: string; values: unknown[] }> }) => {
    const f = frame.fields.find((x) => x.type === 'string');
    return (f?.values ?? []).map((v) => ({ text: v }));
  },
}));

import { of } from 'rxjs';
import { DataSourceInstanceSettings } from '@grafana/data';
import { ArcDataSource } from './datasource';
import { ArcDataSourceOptions } from './types';
import { resolveMode } from './builder/mode';

function showTablesFrame() {
  return {
    fields: [
      { name: 'database', type: 'string', values: ['telegraf', 'telegraf'] },
      { name: 'table_name', type: 'string', values: ['cpu', 'mem'] },
      { name: 'file_count', type: 'number', values: [3, 1] },
    ],
  };
}

function datasourceAnswering(frame: object) {
  const ds = new ArcDataSource({ jsonData: {} } as unknown as DataSourceInstanceSettings<ArcDataSourceOptions>);
  const query = jest.spyOn(ds, 'query').mockReturnValue(of({ data: [frame] }));
  return { ds, query };
}

test('a SHOW TABLES variable lists the tables, not the database', async () => {
  for (const sql of ['SHOW TABLES', 'show measurements from telegraf;']) {
    const { ds } = datasourceAnswering(showTablesFrame());
    expect((await ds.metricFindQuery(sql)).map((v) => v.text)).toEqual(['cpu', 'mem']);
  }
});

test('other variable queries keep the first text column', async () => {
  const { ds } = datasourceAnswering(showTablesFrame());
  expect((await ds.metricFindQuery('SELECT database, table_name FROM x')).map((v) => v.text)).toEqual(['telegraf']);
});

describe('builder variables', () => {
  const builderVar = (patch: object, query: string) => ({
    query,
    editorMode: 'builder',
    builder: { version: 1, kind: 'values', where: [], sql: query, ...patch },
  });
  function ds() {
    const d = new ArcDataSource({ jsonData: {} } as unknown as DataSourceInstanceSettings<ArcDataSourceOptions>);
    const query = jest
      .spyOn(d, 'query')
      .mockReturnValue(of({ data: [{ fields: [{ name: 'value', type: 'string', values: ['a', 'b'] }] }] }));
    jest.spyOn(d, 'getDatabases').mockResolvedValue({ databases: ['metrics'], scoped: true });
    const measurements = jest.spyOn(d, 'getMeasurements').mockResolvedValue(['cpu', 'mem']);
    const columns = jest.spyOn(d, 'getColumns').mockResolvedValue({
      source: 'schema',
      columns: [
        { name: 'time', type: 'TIMESTAMP' },
        { name: 'host', type: 'VARCHAR' },
        { name: 'up', type: 'BOOLEAN' },
        { name: 'usage', type: 'DOUBLE' },
      ],
    });
    return { d, query, measurements, columns };
  }

  test('databases come from the metadata route, scoped fallback included; no SQL runs', async () => {
    const { d, query } = ds();
    expect((await d.metricFindQuery(builderVar({ kind: 'databases' }, 'SHOW DATABASES'))).map((v) => v.text)).toEqual([
      'metrics',
    ]);
    expect(query).not.toHaveBeenCalled();
  });

  test('measurements and tag keys too; tag keys are text columns unless asked otherwise', async () => {
    const { d, measurements, columns } = ds();
    expect(
      (
        await d.metricFindQuery(
          builderVar({ kind: 'measurements', database: 'telegraf' }, 'SHOW TABLES FROM "telegraf"')
        )
      ).map((v) => v.text)
    ).toEqual(['cpu', 'mem']);
    expect(measurements).toHaveBeenCalledWith('telegraf');
    const keys = builderVar({ kind: 'columns', measurement: 'cpu' }, '-- Arc builder: text columns of cpu');
    expect((await d.metricFindQuery(keys)).map((v) => v.text)).toEqual(['host', 'up']);
    expect(columns).toHaveBeenCalledWith('', 'cpu');
    const all = builderVar({ kind: 'columns', measurement: 'cpu', textOnly: false }, '-- Arc builder: columns of cpu');
    expect((await d.metricFindQuery(all)).map((v) => v.text)).toEqual(['host', 'up', 'usage']);
  });

  test('tag values run as builder SQL, so a chained single-value variable becomes a quoted literal (review B3)', async () => {
    const { d, query } = ds();
    const sql = `SELECT DISTINCT "host" AS "value"\nFROM "cpu"\nWHERE $__timeFilter(time) AND "region" IN ($region) AND "host" IS NOT NULL\nORDER BY 1\nLIMIT 1000`;
    const values = await d.metricFindQuery(builderVar({ measurement: 'cpu', column: 'host' }, sql));
    expect(values.map((v) => v.text)).toEqual(['a', 'b']);
    const target = query.mock.calls[0][0].targets[0];
    expect(target.editorMode).toBe('builder');
    expect(target.builder?.sql).toBe(target.sql);
    expect(resolveMode(target)).toBe('builder');
  });

  test('a builder variable whose SQL was edited by hand runs as Code', async () => {
    const { d, query } = ds();
    const edited = { ...builderVar({ kind: 'databases' }, 'SHOW DATABASES'), query: 'SELECT 1 AS x' };
    await d.metricFindQuery(edited);
    expect(query.mock.calls[0][0].targets[0].sql).toBe('SELECT 1 AS x');
  });
});
