import React from 'react';
import { QueryEditorProps } from '@grafana/data';
import { InlineField, Input, TextArea, RadioButtonGroup, Select } from '@grafana/ui';
import { ArcDataSource } from './datasource';
import { ArcDataSourceOptions, ArcQuery } from './types';

type Props = QueryEditorProps<ArcDataSource, ArcQuery, ArcDataSourceOptions>;

const FORMAT_OPTIONS = [
  { label: 'Time series', value: 'time_series' },
  { label: 'Table', value: 'table' },
];

const SPLIT_OPTIONS = [
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
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const onSQLChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, sql: event.target.value });
  };

  const onFormatChange = (value: string) => {
    onChange({ ...query, format: value as 'time_series' | 'table' });
    onRunQuery();
  };

  const onSplitChange = (option: any) => {
    onChange({ ...query, splitDuration: option?.value || 'off' });
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
      <InlineField
        label="Format"
        labelWidth={14}
        tooltip="Choose how to format the query results"
      >
        <RadioButtonGroup
          options={FORMAT_OPTIONS}
          value={query.format || 'time_series'}
          onChange={onFormatChange}
        />
      </InlineField>

      <InlineField
        label="Query splitting"
        labelWidth={14}
        tooltip="Split large time ranges into parallel chunks. Use for slow queries over long periods (e.g. 30d+). Each chunk runs in parallel against Arc."
      >
        <Select
          options={SPLIT_OPTIONS}
          value={query.splitDuration || 'off'}
          onChange={onSplitChange}
          width={20}
        />
      </InlineField>

      <InlineField
        label="Database"
        labelWidth={14}
        tooltip="Override the default database for this query. Leave empty to use the datasource default."
      >
        <Input
          value={query.database || ''}
          onChange={onDatabaseChange}
          onBlur={onDatabaseBlur}
          placeholder="default"
          width={20}
        />
      </InlineField>

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
            <strong>Available Macros:</strong> $__timeFilter(time), $__timeFrom(), $__timeTo(), $__interval, $__timeGroup(column, interval)
          </div>
          <div style={{ fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '11px', color: '#888' }}>
            Example: SELECT $__timeGroup(time, &apos;$__interval&apos;) AS time, host, AVG(value) FROM metrics WHERE $__timeFilter(time) GROUP BY 1, host ORDER BY 1
          </div>
        </div>
      </div>
    </div>
  );
}
