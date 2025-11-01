import React from 'react';
import { QueryEditorProps } from '@grafana/data';
import { InlineField, TextArea, RadioButtonGroup } from '@grafana/ui';
import { ArcDataSource } from './datasource';
import { ArcDataSourceOptions, ArcQuery } from './types';

type Props = QueryEditorProps<ArcDataSource, ArcQuery, ArcDataSourceOptions>;

const FORMAT_OPTIONS = [
  { label: 'Time series', value: 'time_series' },
  { label: 'Table', value: 'table' },
];

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onSQLChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, sql: event.target.value });
  };

  const onFormatChange = (value: string) => {
    onChange({ ...query, format: value as 'time_series' | 'table' });
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
            <strong>Available Macros:</strong> $__timeFilter(time), $__timeFrom(), $__timeTo(), $__interval
          </div>
          <div style={{ fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '11px', color: '#888' }}>
            Example: SELECT time, host, AVG(value) FROM metrics WHERE $__timeFilter(time) GROUP BY time_bucket(&apos;$__interval&apos;, time), host ORDER BY time
          </div>
        </div>
      </div>
    </div>
  );
}
