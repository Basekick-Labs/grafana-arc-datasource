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
    <div className="gf-form-group">
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
          placeholder="SELECT DISTINCT host FROM cpu ORDER BY host"
          rows={4}
          style={{
            fontFamily: 'monospace',
            fontSize: '13px'
          }}
        />
      </InlineField>

      <div className="gf-form">
        <label className="gf-form-label width-20"></label>
        <div className="gf-form-label">
          <small style={{ color: '#999' }}>
            <strong>Examples:</strong>
            <br />
            • Get distinct hosts: SELECT DISTINCT host FROM cpu ORDER BY host
            <br />
            • Get tables: SHOW TABLES
            <br />
            • Get databases: SHOW DATABASES
            <br />
            • Custom values: SELECT name FROM measurements WHERE type = 'cpu'
          </small>
        </div>
      </div>
    </div>
  );
};
