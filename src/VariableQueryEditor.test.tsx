const templateVariables: Array<{ name: string }> = [];
jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    constructor(public instanceSettings: unknown) {}
  },
  frameToMetricFindValue: () => [],
  getTemplateSrv: () => ({ replace: (s: string) => s, getVariables: () => templateVariables }),
}));

import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { VariableQueryEditor } from './VariableQueryEditor';
import { ArcDataSource } from './datasource';

const datasource = {
  getDatabases: jest.fn(async () => ({ databases: ['metrics'], scoped: false })),
  getMeasurements: jest.fn(async () => ['cpu']),
  getColumns: jest.fn(async () => ({ source: 'schema', columns: [{ name: 'host', type: 'VARCHAR' }] })),
  allowsDatabaseOverride: () => true,
  defaultDatabase: () => 'metrics',
} as unknown as ArcDataSource;

test('a variable saved before the builder opens in Code, untouched', () => {
  const onChange = jest.fn();
  render(
    <VariableQueryEditor
      query={{ query: 'SELECT DISTINCT host FROM cpu' }}
      onChange={onChange}
      datasource={datasource}
    />
  );
  expect(screen.getByDisplayValue('SELECT DISTINCT host FROM cpu')).toBeInTheDocument();
  expect(screen.queryByTestId('arc-variable-builder')).toBeNull();
  expect(onChange).not.toHaveBeenCalled();
});

test('a new variable opens in the builder; picking a kind writes its query', () => {
  const onChange = jest.fn();
  render(<VariableQueryEditor query={{ query: '' }} onChange={onChange} datasource={datasource} />);
  expect(screen.getByTestId('arc-variable-builder')).toBeInTheDocument();
  expect(screen.getByTestId('arc-variable-errors')).toHaveTextContent('Pick a measurement');
  fireEvent.click(screen.getByLabelText('Databases'));
  const [saved, definition] = onChange.mock.calls[onChange.mock.calls.length - 1];
  expect(saved).toMatchObject({
    query: 'SHOW DATABASES',
    editorMode: 'builder',
    builder: { kind: 'databases', sql: 'SHOW DATABASES' },
  });
  expect(definition).toBe('Arc databases');
  expect(screen.getByTestId('arc-variable-sql')).toHaveTextContent('SHOW DATABASES');
});

test('Code to Builder asks first when the SQL is not the builder’s', async () => {
  const onChange = jest.fn();
  render(<VariableQueryEditor query={{ query: 'SELECT 1' }} onChange={onChange} datasource={datasource} />);
  fireEvent.click(screen.getByLabelText('Builder'));
  expect(await screen.findByText(/cannot read this SQL/)).toBeInTheDocument();
  fireEvent.click(screen.getByText('Discard code and switch'));
  await waitFor(() => expect(screen.getByTestId('arc-variable-builder')).toBeInTheDocument());
  expect(onChange.mock.calls[0][0]).toMatchObject({ editorMode: 'builder', query: '' });
});
