import React, { useState } from 'react';
import { InlineField, TextArea } from '@grafana/ui';

interface VariableQuery {
  query: string;
}

interface VariableQueryProps {
  query: VariableQuery;
  onChange: (query: VariableQuery, definition: string) => void;
}

export const VariableQueryEditor: React.FC<VariableQueryProps> = ({ query, onChange }) => {
  const [state, setState] = useState(query);

  const saveQuery = () => {
    onChange(state, state.query);
  };

  const handleChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    setState({
      ...state,
      query: event.target.value,
    });
  };

  return (
    <>
      <InlineField
        label="Query"
        labelWidth={20}
        tooltip="SQL query to generate variable values. First column will be used."
        grow
      >
        <TextArea
          value={state.query || ''}
          onChange={handleChange}
          onBlur={saveQuery}
          placeholder="SELECT DISTINCT host FROM telegraf.cpu ORDER BY host"
          rows={3}
          style={{
            fontFamily: 'monospace',
            fontSize: '13px',
            width: '100%'
          }}
        />
      </InlineField>

      <div style={{ marginTop: '8px', marginLeft: '20px', paddingLeft: '8px' }}>
        <small style={{ color: '#6e6e6e', display: 'block', lineHeight: '1.6' }}>
          <strong>Examples:</strong>
          <br />
          • Get distinct hosts: <code style={{ fontSize: '12px' }}>SELECT DISTINCT host FROM telegraf.cpu ORDER BY host</code>
          <br />
          • Get tables: <code style={{ fontSize: '12px' }}>SHOW TABLES</code>
          <br />
          • Get databases: <code style={{ fontSize: '12px' }}>SHOW DATABASES</code>
        </small>
      </div>
    </>
  );
};
