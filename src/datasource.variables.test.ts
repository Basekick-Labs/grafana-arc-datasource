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
