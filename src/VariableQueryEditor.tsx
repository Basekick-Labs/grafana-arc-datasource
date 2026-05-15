import React, { useEffect, useState } from 'react';
import { InlineField, TextArea, useStyles2 } from '@grafana/ui';
import { GrafanaTheme2 } from '@grafana/data';
import { css } from '@emotion/css';

interface VariableQuery {
  query: string;
}

interface VariableQueryProps {
  query: VariableQuery;
  onChange: (query: VariableQuery, definition: string) => void;
}

export function VariableQueryEditor({ query, onChange }: VariableQueryProps) {
  const styles = useStyles2(getStyles);
  const [state, setState] = useState(query);

  // Sync local state when the SQL content of the parent's `query` prop
  // changes (e.g. a different variable is selected in the dropdown above
  // this editor, or the dashboard JSON is edited externally). The
  // dependency is `query.query` specifically — NOT the `query` object —
  // because the parent re-renders on every dashboard refresh and may
  // pass a fresh object reference with the same content, which would
  // overwrite the user's in-progress unsaved typing. (R1 H6, gemini
  // round-3 follow-up.)
  useEffect(() => {
    setState(query);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query.query]);

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
          className={styles.textarea}
          value={state.query || ''}
          onChange={handleChange}
          onBlur={saveQuery}
          placeholder="SELECT DISTINCT host FROM telegraf.cpu ORDER BY host"
          rows={3}
        />
      </InlineField>

      <div className={styles.examples}>
        <small>
          <strong>Examples:</strong>
          <br />• Get distinct hosts: <code className={styles.code}>SELECT DISTINCT host FROM telegraf.cpu ORDER BY host</code>
          <br />• Get tables: <code className={styles.code}>SHOW TABLES</code>
          <br />• Get databases: <code className={styles.code}>SHOW DATABASES</code>
        </small>
      </div>
    </>
  );
}

const getStyles = (theme: GrafanaTheme2) => ({
  textarea: css({
    fontFamily: theme.typography.fontFamilyMonospace,
    fontSize: '13px',
    width: '100%',
  }),
  examples: css({
    marginTop: theme.spacing(1),
    marginLeft: theme.spacing(2.5),
    paddingLeft: theme.spacing(1),
    color: theme.colors.text.secondary,
    lineHeight: 1.6,
  }),
  code: css({
    fontSize: '12px',
  }),
});
