import React from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { InlineField, Input, TextArea, RadioButtonGroup, Select } from '@grafana/ui';
import { ArcDataSource } from './datasource';
import { ArcDataSourceOptions, ArcQuery } from './types';

type Props = QueryEditorProps<ArcDataSource, ArcQuery, ArcDataSourceOptions>;

const FORMAT_OPTIONS = [
  { label: 'Time series', value: 'time_series' },
  { label: 'Table', value: 'table' },
];

const SPLIT_OPTIONS = [
  { label: 'Auto', value: 'auto' },
  { label: 'Off', value: 'off' },
  { label: '1 hour', value: '1h' },
  { label: '6 hours', value: '6h' },
  { label: '12 hours', value: '12h' },
  { label: '1 day', value: '1d' },
  { label: '3 days', value: '3d' },
  { label: '7 days', value: '7d' },
];

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  // Migrate rawSql from Postgres/MySQL/MSSQL/ClickHouse datasources
  React.useEffect(() => {
    if (!query.sql && query.rawSql) {
      onChange({ ...query, sql: query.rawSql, rawSql: undefined });
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps -- intentionally runs only on mount for one-time rawSql migration

  const onSQLChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, sql: event.target.value });
  };

  const onFormatChange = (value: string) => {
    onChange({ ...query, format: value as 'time_series' | 'table' });
    onRunQuery();
  };

  const onSplitChange = (option: SelectableValue<string>) => {
    onChange({ ...query, splitDuration: option?.value || 'auto' });
    onRunQuery();
  };

  const onDatabaseChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, database: event.target.value });
  };

  const onDatabaseBlur = () => {
    onRunQuery();
  };

  return (
    <div className="gf-form-group">
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 16px', alignItems: 'center', marginBottom: '8px' }}>
        <InlineField
          label="Format"
          tooltip="Choose how to format the query results"
        >
          <RadioButtonGroup
            options={FORMAT_OPTIONS}
            value={query.format || 'time_series'}
            onChange={onFormatChange}
          />
        </InlineField>

        <InlineField
          label="Splitting"
          tooltip="Parallel time-range chunking for faster results. Applies to: time-bucketed ($__timeGroup) and raw queries. Auto-skipped for: GROUP BY, DISTINCT, COUNT/SUM/AVG without $__timeGroup, LIMIT, and no $__timeFilter."
        >
          <Select
            options={SPLIT_OPTIONS}
            value={query.splitDuration || 'auto'}
            onChange={onSplitChange}
            width={16}
          />
        </InlineField>

        <InlineField
          label="Database"
          tooltip="Override the default database for this query. Leave empty to use the datasource default."
        >
          <Input
            value={query.database || ''}
            onChange={onDatabaseChange}
            onBlur={onDatabaseBlur}
            placeholder="default"
            width={16}
          />
        </InlineField>
      </div>

      <div className="gf-form" style={{ flexDirection: 'column', alignItems: 'flex-start' }}>
        <label className="gf-form-label" style={{ marginBottom: '8px' }}>SQL Query</label>
        <TextArea
          value={query.sql || ''}
          onChange={onSQLChange}
          onBlur={onRunQuery}
          placeholder="SELECT * FROM systems.cpu ORDER BY time DESC LIMIT 100"
          rows={8}
          style={{
            width: '100%',
            fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace',
            fontSize: '14px',
            lineHeight: '1.5'
          }}
        />
        <div style={{ marginTop: '8px', fontSize: '12px', color: '#6e6e6e' }}>
          <div style={{ marginBottom: '4px' }}>
            <strong>Available Macros:</strong> $__timeFilter(column), $__timeFrom(), $__timeTo(), $__interval, $__timeGroup(column, interval)
          </div>
          <div style={{ marginBottom: '4px', fontSize: '11px', color: '#888' }}>
            $__timeGroup intervals: &apos;$__interval&apos; (auto), &apos;1 hour&apos;, &apos;10 minutes&apos;, &apos;1 minute&apos;, &apos;10 seconds&apos;, &apos;1 day&apos; — or short forms: &apos;1h&apos;, &apos;10m&apos;, &apos;1m&apos;, &apos;1d&apos;
          </div>
          <div style={{ fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '11px', color: '#888' }}>
            Example: SELECT $__timeGroup(time, &apos;$__interval&apos;) AS time, host, AVG(value) FROM metrics WHERE $__timeFilter(time) GROUP BY 1, host ORDER BY 1
          </div>
        </div>
      </div>
    </div>
  );
}
