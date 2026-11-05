const templateVariables: Array<{ name: string; multi?: boolean; includeAll?: boolean; allValue?: string }> = [];
jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    constructor(public instanceSettings: unknown) {}
  },
  frameToMetricFindValue: () => [],
  getTemplateSrv: () => ({
    replace: (s: string) => s,
    getVariables: () => templateVariables,
  }),
}));

import React, { useState } from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryEditor } from '../QueryEditor';
import { ArcDataSource, ColumnsResponse } from '../datasource';
import { ArcQuery, GENERATOR_VERSION, builderDefaultQuery, emptyBuilder } from '../types';

const CPU: ColumnsResponse = {
  source: 'schema',
  columns: [
    { name: 'time', type: 'TIMESTAMP WITH TIME ZONE' },
    { name: 'host', type: 'VARCHAR' },
    { name: 'region', type: 'VARCHAR' },
    { name: 'usage_idle', type: 'DOUBLE' },
    { name: 'ok', type: 'BOOLEAN' },
  ],
};
const MEM: ColumnsResponse = {
  source: 'schema',
  columns: [
    { name: 'time', type: 'TIMESTAMP WITH TIME ZONE' },
    { name: 'host', type: 'VARCHAR' },
    { name: 'used', type: 'BIGINT' },
  ],
};

function fakeDatasource() {
  return {
    getDatabases: jest.fn(async () => ({ databases: ['metrics', 'other'], scoped: false })),
    getMeasurements: jest.fn(async () => ['cpu', 'mem']),
    getColumns: jest.fn(async (_db: string, m: string) => (m === 'mem' ? MEM : CPU)),
    getValues: jest.fn(async () => ({ values: ['a', 'b'], more: false })),
    allowsDatabaseOverride: () => true,
    defaultDatabase: () => 'metrics',
  } as unknown as ArcDataSource & { getColumns: jest.Mock; getValues: jest.Mock };
}

/** Renders the editor with real state, so every onChange feeds the next render. */
function Harness({
  initial,
  ds,
  onRun,
  onSeen,
}: {
  initial: ArcQuery;
  ds: ArcDataSource;
  onRun: () => void;
  onSeen: (q: ArcQuery) => void;
}) {
  const [query, setQuery] = useState(initial);
  return (
    <QueryEditor
      query={query}
      datasource={ds}
      onChange={(q) => {
        onSeen(q);
        setQuery(q);
      }}
      onRunQuery={onRun}
    />
  );
}

function setup(initial: ArcQuery) {
  const ds = fakeDatasource();
  const onRun = jest.fn();
  const seen: ArcQuery[] = [];
  render(<Harness initial={initial} ds={ds} onRun={onRun} onSeen={(q) => seen.push(q)} />);
  return { ds, onRun, last: () => seen[seen.length - 1] };
}

async function pick(segmentText: string, option: string) {
  fireEvent.click(screen.getByText(segmentText));
  fireEvent.click(await screen.findByText(option));
}

beforeEach(() => {
  templateVariables.length = 0;
});

test('a new query opens in the builder; picking a measurement writes and runs SQL', async () => {
  const { ds, onRun, last } = setup({ ...(builderDefaultQuery() as ArcQuery), refId: 'A' });
  expect(screen.getByTestId('arc-query-builder')).toBeInTheDocument();
  expect(screen.getByTestId('arc-builder-errors')).toHaveTextContent('Pick a measurement');

  await pick('select measurement', 'cpu');
  await waitFor(() => expect(last()?.sql).toContain('avg("usage_idle") AS "mean"'));
  expect(ds.getColumns).toHaveBeenCalledWith('', 'cpu');
  expect(last().builder?.sql).toBe(last().sql);
  expect(last().editorMode).toBe('builder');
  expect(onRun).toHaveBeenCalled();
  expect(screen.getByTestId('arc-builder-sql')).toHaveTextContent('FROM "cpu"');
});

test('changing the format regenerates the builder SQL', async () => {
  const { last } = setup({ ...(builderDefaultQuery() as ArcQuery), refId: 'A' });
  await pick('select measurement', 'cpu');
  await waitFor(() => expect(last()?.sql).toContain('ORDER BY 1'));
  fireEvent.click(screen.getByLabelText('Table'));
  await waitFor(() => expect(last().format).toBe('table'));
  expect(last().builder?.sql).toBe(last().sql);
});

test('a measurement change drops what the new one does not have, and says so', async () => {
  const sql0 = '';
  const { last } = setup({
    refId: 'A',
    editorMode: 'builder',
    sql: sql0,
    builder: {
      ...emptyBuilder(),
      measurement: 'cpu',
      select: [{ column: 'usage_idle', fn: 'mean' }],
      where: [{ column: 'region', op: '=', value: 'eu', valueType: 'string' }],
      groupBy: { time: { interval: '$__interval' }, tags: ['host'] },
      sql: sql0,
    },
  });
  await pick('cpu', 'mem');
  await waitFor(() => expect(last()?.builder?.measurement).toBe('mem'));
  expect(last().builder?.where).toEqual([]);
  expect(last().builder?.groupBy.tags).toEqual(['host']);
  expect(last().builder?.select).toEqual([{ column: 'used', fn: 'mean' }]);
  expect(await screen.findByText(/Not in mem, removed: usage_idle, region/)).toBeInTheDocument();
});

test('a legacy query opens in Code, untouched', () => {
  const { last } = setup({ refId: 'A', sql: 'SELECT 1', format: 'table' });
  expect(screen.queryByTestId('arc-query-builder')).not.toBeInTheDocument();
  expect(screen.getByDisplayValue('SELECT 1')).toBeInTheDocument();
  expect(last()).toBeUndefined();
});

test('builder defaults merged over a legacy alert query stay in Code, and are not saved on edit', () => {
  const merged = { ...(builderDefaultQuery() as ArcQuery), refId: 'A', sql: 'SELECT avg(v) FROM cpu' };
  const { last } = setup(merged);
  expect(screen.queryByTestId('arc-query-builder')).not.toBeInTheDocument();
  fireEvent.change(screen.getByDisplayValue('SELECT avg(v) FROM cpu'), { target: { value: 'SELECT max(v) FROM cpu' } });
  expect(last()).toEqual({ refId: 'A', format: 'time_series', sql: 'SELECT max(v) FROM cpu' });
});

test('Code to Builder asks first, then restores the last builder state', async () => {
  const sql = 'SELECT time, "usage_idle"\nFROM "cpu"\nWHERE $__timeFilter(time)\nORDER BY time ASC';
  const { last } = setup({
    refId: 'A',
    editorMode: 'code',
    sql: sql + ' -- edited',
    builder: { ...emptyBuilder(), measurement: 'cpu', select: [{ column: 'usage_idle', fn: 'raw' }], sql },
  });
  fireEvent.click(screen.getByLabelText('Builder'));
  expect(await screen.findByText(/not carried into the builder/)).toBeInTheDocument();
  fireEvent.click(screen.getByText('Discard code and switch'));
  await waitFor(() => expect(last()?.editorMode).toBe('builder'));
  expect(last().sql).toBe(sql);
  expect(screen.getByTestId('arc-query-builder')).toBeInTheDocument();
});

test('Builder to Code keeps the SQL', async () => {
  const { last } = setup({ ...(builderDefaultQuery() as ArcQuery), refId: 'A' });
  await pick('select measurement', 'cpu');
  await waitFor(() => expect(last()?.sql).not.toBe(''));
  const sql = last().sql;
  fireEvent.click(screen.getByLabelText('Code'));
  await waitFor(() => expect(last().editorMode).toBe('code'));
  expect(last().sql).toBe(sql);
  expect(screen.getByDisplayValue(sql.split('\n')[0], { exact: false })).toBeInTheDocument();
});

test('a multi-value variable with LIKE is refused and the query is not run', async () => {
  templateVariables.push({ name: 'host', multi: true });
  const { onRun } = setup({
    refId: 'A',
    editorMode: 'builder',
    sql: '',
    builder: {
      ...emptyBuilder(),
      measurement: 'cpu',
      select: [{ column: 'usage_idle', fn: 'mean' }],
      where: [{ column: 'host', op: 'LIKE', value: '$host', valueType: 'string' }],
      sql: '',
    },
  });
  await act(async () => {});
  expect(screen.getByTestId('arc-builder-errors')).toHaveTextContent('can hold several values');
  expect(onRun).not.toHaveBeenCalled();
});

test('a key that cannot list databases gets the configured one and a hint to type a name', async () => {
  const ds = fakeDatasource();
  (ds.getDatabases as jest.Mock).mockImplementation(async () => ({ databases: ['metrics'], scoped: true }));
  render(
    <Harness
      initial={{ ...(builderDefaultQuery() as ArcQuery), refId: 'A' }}
      ds={ds}
      onRun={jest.fn()}
      onSeen={() => {}}
    />
  );
  fireEvent.click(screen.getByText('metrics'));
  expect(await screen.findByText('Type a name…')).toBeInTheDocument();
  expect(screen.getByText('This API key cannot list every database')).toBeInTheDocument();
  expect(screen.queryByText('other')).not.toBeInTheDocument();
});

test('a transformation and math are picked per field, and the SQL follows', async () => {
  const { last } = setup({ ...(builderDefaultQuery() as ArcQuery), refId: 'A' });
  await pick('select measurement', 'cpu');
  await waitFor(() => expect(last()?.sql).toContain('avg("usage_idle") AS "mean"'));
  await pick('transform', 'non_negative_derivative');
  await waitFor(() => expect(last().sql).toContain('WITH arc_builder_buckets AS ('));
  expect(last().builder?.select[0].transform).toEqual({ kind: 'non_negative_derivative', n: undefined });
  expect(last().sql).toContain('AS "non_negative_derivative"');
  expect(last().builder?.generatorVersion).toBe(GENERATOR_VERSION);

  fireEvent.click(screen.getByText('math'));
  const input = await screen.findByPlaceholderText('* 8 / 1024');
  fireEvent.change(input, { target: { value: '* 8' } });
  fireEvent.blur(input);
  await waitFor(() => expect(last().builder?.select[0].math).toBe('* 8'));
  expect(last().sql).toMatch(/\* 8\) AS "non_negative_derivative"/);

  // The alias placeholder shows the same name; the transformation segment comes first.
  fireEvent.click(screen.getAllByText('non_negative_derivative')[0]);
  fireEvent.click(await screen.findByRole('option', { name: /^none/ }));
  await waitFor(() => expect(last().sql).not.toContain('arc_builder_buckets'));
});

test('a transformation is offered only where values come in time order', async () => {
  setup({
    refId: 'A',
    editorMode: 'builder',
    format: 'table',
    sql: '',
    builder: {
      ...emptyBuilder(),
      measurement: 'cpu',
      select: [{ column: 'usage_idle', fn: 'mean' }],
      groupBy: { tags: [] },
      sql: '',
    },
  });
  await waitFor(() => expect(screen.getByTestId('arc-builder-sql')).toHaveTextContent('avg("usage_idle")'));
  fireEvent.click(screen.getByText('transform'));
  expect(
    await screen.findByText(/Rate of change per unit: needs GROUP BY time, or a raw time series/)
  ).toBeInTheDocument();
});
