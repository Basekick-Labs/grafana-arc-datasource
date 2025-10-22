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
        labelWidth={20}
        tooltip="Choose how to format the query results"
      >
        <RadioButtonGroup
          options={FORMAT_OPTIONS}
          value={query.format || 'time_series'}
          onChange={onFormatChange}
        />
      </InlineField>

      <div className="gf-form">
        <label className="gf-form-label width-20">SQL Query</label>
        <div className="gf-form-input" style={{ width: '100%' }}>
          <TextArea
            value={query.sql || ''}
            onChange={onSQLChange}
            onBlur={onRunQuery}
            placeholder="SELECT * FROM cpu WHERE $__timeFilter(time) LIMIT 100"
            rows={10}
            style={{
              width: '100%',
              fontFamily: 'monospace',
              fontSize: '13px'
            }}
          />
        </div>
      </div>

      <div className="gf-form">
        <label className="gf-form-label width-20"></label>
        <div className="gf-form-label">
          <small style={{ color: '#999' }}>
            <strong>Macros:</strong> $__timeFilter(column), $__timeFrom(), $__timeTo(), $__interval
            <br />
            <strong>Example:</strong> SELECT time, AVG(usage_idle) FROM cpu WHERE $__timeFilter(time) GROUP BY time_bucket(INTERVAL '$__interval', time)
          </small>
        </div>
      </div>
    </div>
  );
}
