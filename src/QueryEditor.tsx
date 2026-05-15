import React, { useEffect } from 'react';
import { GrafanaTheme2, QueryEditorProps, SelectableValue } from '@grafana/data';
import { InlineField, Input, TextArea, RadioButtonGroup, Select, useStyles2 } from '@grafana/ui';
import { css } from '@emotion/css';
import { ArcDataSource } from './datasource';
import { ArcDataSourceOptions, ArcQuery } from './types';

type Props = QueryEditorProps<ArcDataSource, ArcQuery, ArcDataSourceOptions>;

const FORMAT_OPTIONS = [
  { label: 'Time series', value: 'time_series' as const },
  { label: 'Table', value: 'table' as const },
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
  const styles = useStyles2(getStyles);

  // One-time migration: dashboards copied from Postgres / MySQL / MSSQL /
  // ClickHouse use `rawSql`; Arc uses `sql`. Pull the old field over once
  // on mount. The disable note: this is the rare case where exhaustive-
  // deps would force the migration to re-fire on every prop change,
  // which is wrong — we only want it once per editor mount.
  useEffect(() => {
    if (!query.sql && query.rawSql) {
      onChange({ ...query, sql: query.rawSql, rawSql: undefined });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const onSQLChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, sql: event.target.value });
  };

  const onFormatChange = (value: 'time_series' | 'table') => {
    onChange({ ...query, format: value });
    onRunQuery();
  };

  const onSplitChange = (option: SelectableValue<string>) => {
    onChange({ ...query, splitDuration: option?.value || 'auto' });
    onRunQuery();
  };

  const onDatabaseChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, database: event.target.value });
  };

  return (
    <div className="gf-form-group">
      <div className={styles.toolbar}>
        <InlineField label="Format" tooltip="Choose how to format the query results">
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
          tooltip="Override the default database for this query. Leave empty to use the datasource default. The datasource setting 'Allow Database Override' must be enabled."
        >
          <Input
            value={query.database || ''}
            onChange={onDatabaseChange}
            onBlur={onRunQuery}
            placeholder="default"
            width={16}
          />
        </InlineField>
      </div>

      <div className={styles.sqlBlock}>
        <label className="gf-form-label">SQL Query</label>
        <TextArea
          className={styles.sqlInput}
          value={query.sql || ''}
          onChange={onSQLChange}
          onBlur={onRunQuery}
          placeholder="SELECT * FROM systems.cpu ORDER BY time DESC LIMIT 100"
          rows={8}
        />
        <div className={styles.help}>
          <div className={styles.helpLine}>
            <strong>Available Macros:</strong> $__timeFilter(column), $__timeFrom(), $__timeTo(), $__interval, $__timeGroup(column, interval)
          </div>
          <div className={styles.helpHint}>
            $__timeGroup intervals: &apos;$__interval&apos; (auto), &apos;1 hour&apos;, &apos;10 minutes&apos;, &apos;1 minute&apos;, &apos;10 seconds&apos;, &apos;1 day&apos; — or short forms: &apos;1h&apos;, &apos;10m&apos;, &apos;1m&apos;, &apos;1d&apos;
          </div>
          <div className={styles.helpExample}>
            Example: SELECT $__timeGroup(time, &apos;$__interval&apos;) AS time, host, AVG(value) FROM metrics WHERE $__timeFilter(time) GROUP BY 1, host ORDER BY 1
          </div>
        </div>
      </div>
    </div>
  );
}

const getStyles = (theme: GrafanaTheme2) => ({
  toolbar: css({
    display: 'flex',
    flexWrap: 'wrap',
    gap: theme.spacing(0.5, 2),
    alignItems: 'center',
    marginBottom: theme.spacing(1),
  }),
  sqlBlock: css({
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'flex-start',
  }),
  sqlInput: css({
    width: '100%',
    fontFamily: theme.typography.fontFamilyMonospace,
    fontSize: '14px',
    lineHeight: 1.5,
    marginTop: theme.spacing(1),
  }),
  help: css({
    marginTop: theme.spacing(1),
    fontSize: '12px',
    color: theme.colors.text.secondary,
  }),
  helpLine: css({
    marginBottom: theme.spacing(0.5),
  }),
  helpHint: css({
    marginBottom: theme.spacing(0.5),
    fontSize: '11px',
    color: theme.colors.text.disabled,
  }),
  helpExample: css({
    fontFamily: theme.typography.fontFamilyMonospace,
    fontSize: '11px',
    color: theme.colors.text.disabled,
  }),
});
