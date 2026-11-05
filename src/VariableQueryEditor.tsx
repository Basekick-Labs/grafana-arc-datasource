import React, { useState } from 'react';
import {
  Button,
  Combobox,
  ComboboxOption,
  ConfirmModal,
  IconButton,
  InlineField,
  InlineSwitch,
  Input,
  RadioButtonGroup,
  TextArea,
  useStyles2,
} from '@grafana/ui';
import { GrafanaTheme2 } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import { css } from '@emotion/css';
import { ArcDataSource } from './datasource';
import { Filter, FilterOp } from './types';
import { kindOf, operatorsFor, valueTypeFor } from './builder/columns';
import { rawSingle } from './builder/interpolate';
import {
  ArcVariableQuery,
  VariableBuilder,
  VariableKind,
  emptyVariableBuilder,
  generateVariable,
  resolveVariableMode,
  toVariableQuery,
  variableDefinition,
} from './builder/variableQuery';

interface VariableQueryProps {
  query: unknown;
  onChange: (query: ArcVariableQuery, definition: string) => void;
  datasource: ArcDataSource;
}

const MODES = [
  { label: 'Builder', value: 'builder' as const },
  { label: 'Code', value: 'code' as const },
];

const KINDS: Array<{ label: string; value: VariableKind; description: string }> = [
  { label: 'Tag values', value: 'values', description: 'Distinct values of a column (SHOW TAG VALUES)' },
  { label: 'Tag keys', value: 'columns', description: 'Columns of a measurement (SHOW TAG KEYS)' },
  { label: 'Measurements', value: 'measurements', description: 'Measurements in a database (SHOW MEASUREMENTS)' },
  { label: 'Databases', value: 'databases', description: 'Databases this key can read (SHOW DATABASES)' },
];

/** The kinds a builder variable can be, and a Code mode for anything else. */
export function VariableQueryEditor({ query, onChange, datasource }: VariableQueryProps) {
  const styles = useStyles2(getStyles);
  const vq = toVariableQuery(query);
  const [mode, setMode] = useState(resolveVariableMode(vq));
  const [confirmBuilder, setConfirmBuilder] = useState(false);

  const toCode = () => {
    setMode('code');
    onChange({ ...vq, editorMode: 'code' }, vq.query);
  };
  const toBuilder = () => {
    setConfirmBuilder(false);
    setMode('builder');
    const b = vq.builder ?? emptyVariableBuilder();
    const q = generateVariable(b).query ?? '';
    onChange({ query: q, editorMode: 'builder', builder: { ...b, sql: q } }, variableDefinition(b));
  };

  return (
    <>
      <div className={styles.toolbar}>
        <RadioButtonGroup
          options={MODES}
          value={mode}
          size="sm"
          onChange={(m) => {
            if (m === 'code') {
              toCode();
            } else if ((vq.builder && vq.builder.sql === vq.query) || !vq.query.trim()) {
              toBuilder();
            } else {
              setConfirmBuilder(true);
            }
          }}
        />
      </div>
      {mode === 'builder' ? (
        <VariableBuilderEditor
          initial={vq.builder ?? emptyVariableBuilder()}
          datasource={datasource}
          onChange={(b) => {
            const q = generateVariable(b).query ?? '';
            onChange({ query: q, editorMode: 'builder', builder: { ...b, sql: q } }, variableDefinition(b));
          }}
        />
      ) : (
        <VariableCodeEditor
          query={vq.query}
          onChange={(text) => onChange({ ...vq, query: text, editorMode: vq.builder ? 'code' : vq.editorMode }, text)}
        />
      )}
      <ConfirmModal
        isOpen={confirmBuilder}
        title="Switch to the builder?"
        body="The builder cannot read this SQL. Switching discards it and opens the builder's last state."
        confirmText="Discard code and switch"
        onConfirm={toBuilder}
        onDismiss={() => setConfirmBuilder(false)}
      />
    </>
  );
}

/** Resolves a name that may be a variable, for a metadata lookup; undefined when it cannot be. */
function lookupName(raw: string | undefined): string | undefined {
  if (!raw) {
    return '';
  }
  try {
    return getTemplateSrv().replace(raw, undefined, rawSingle);
  } catch {
    return undefined;
  }
}

/** The dashboard's variables, offered as `$name` in every name dropdown. */
function variableOptions(): Array<ComboboxOption<string>> {
  return getTemplateSrv()
    .getVariables()
    .map((v) => ({ label: `$${v.name}`, value: `$${v.name}`, description: 'Variable' }));
}

function VariableBuilderEditor({
  initial,
  datasource,
  onChange,
}: {
  initial: VariableBuilder;
  datasource: ArcDataSource;
  onChange: (b: VariableBuilder) => void;
}) {
  const styles = useStyles2(getStyles);
  const [b, setB] = useState(initial);
  // Column types for the filters' operators, filled as columns load.
  const [types, setTypes] = useState<Record<string, string>>({});
  const commit = (next: VariableBuilder) => {
    setB(next);
    onChange(next);
  };
  const result = generateVariable(b);
  const db = lookupName(b.database);
  const m = lookupName(b.measurement);
  const needsMeasurement = b.kind === 'columns' || b.kind === 'values';

  const matching = (opts: Array<ComboboxOption<string>>, input: string) =>
    opts.filter((o) => !input || o.value.toLowerCase().includes(input.toLowerCase()));
  const loadDatabases = async (input: string): Promise<Array<ComboboxOption<string>>> => {
    const res = await datasource.getDatabases();
    return matching([...res.databases.map((d) => ({ label: d, value: d })), ...variableOptions()], input);
  };
  const loadMeasurements = async (input: string): Promise<Array<ComboboxOption<string>>> => {
    const names = db === undefined ? [] : await datasource.getMeasurements(db);
    return matching([...names.map((n) => ({ label: n, value: n })), ...variableOptions()], input);
  };
  const loadColumns = async (input: string): Promise<Array<ComboboxOption<string>>> => {
    if (!m || db === undefined) {
      return [];
    }
    const res = await datasource.getColumns(db, m);
    const cols = (res.columns ?? []).filter((c) => c.name !== 'time');
    setTypes(Object.fromEntries(cols.map((c) => [c.name, c.type])));
    return matching(
      cols.map((c) => ({ label: c.name, value: c.name, description: c.type })),
      input
    );
  };
  const filterKind = (column: string) => kindOf(types[column]);

  const updateFilter = (i: number, patch: Partial<Filter>) =>
    commit({ ...b, where: b.where.map((f, j) => (j === i ? { ...f, ...patch } : f)) });
  // The first filter joins nothing: drop its connector when one moves up.
  const removeFilter = (i: number) =>
    commit({
      ...b,
      where: b.where
        .filter((_, j) => j !== i)
        .map((f, j) => (j === 0 && f.connector ? { ...f, connector: undefined } : f)),
    });

  return (
    <div className={styles.rows} data-testid="arc-variable-builder">
      <InlineField label="Kind" labelWidth={16}>
        <RadioButtonGroup options={KINDS} value={b.kind} onChange={(kind) => commit({ ...b, kind })} />
      </InlineField>
      {b.kind !== 'databases' && datasource.allowsDatabaseOverride() && (
        <InlineField label="Database" labelWidth={16} tooltip="Leave empty for the datasource's database">
          <Combobox<string>
            value={b.database ?? null}
            placeholder={datasource.defaultDatabase() || 'default'}
            options={loadDatabases}
            createCustomValue
            isClearable
            width={30}
            aria-label="Database"
            onChange={(o) =>
              commit({ ...b, database: o?.value || undefined, measurement: undefined, column: undefined, where: [] })
            }
          />
        </InlineField>
      )}
      {needsMeasurement && (
        <InlineField label="Measurement" labelWidth={16}>
          <Combobox<string>
            value={b.measurement ?? null}
            placeholder="select measurement"
            options={loadMeasurements}
            createCustomValue
            width={30}
            aria-label="Measurement"
            onChange={(o) => commit({ ...b, measurement: o.value, column: undefined, where: [] })}
          />
        </InlineField>
      )}
      {b.kind === 'columns' && (
        <InlineField
          label="Text columns only"
          labelWidth={16}
          tooltip="Text and boolean columns: what InfluxQL calls tags"
        >
          <InlineSwitch
            value={b.textOnly !== false}
            aria-label="Text columns only"
            onChange={(e) => commit({ ...b, textOnly: e.currentTarget.checked })}
          />
        </InlineField>
      )}
      {b.kind === 'values' && (
        <>
          <InlineField label="Column" labelWidth={16}>
            <Combobox<string>
              value={b.column ?? null}
              placeholder="select column"
              options={loadColumns}
              createCustomValue
              width={30}
              aria-label="Column"
              onChange={(o) => commit({ ...b, column: o.value })}
            />
          </InlineField>
          {b.where.map((f, i) => (
            <div key={i} className={styles.filter}>
              <InlineField label={i === 0 ? 'Where' : ''} labelWidth={16}>
                <RadioButtonGroup
                  options={[
                    { label: 'AND', value: 'AND' },
                    { label: 'OR', value: 'OR' },
                  ]}
                  value={f.connector ?? 'AND'}
                  disabled={i === 0}
                  size="sm"
                  onChange={(c) => updateFilter(i, { connector: c === 'OR' ? 'OR' : undefined })}
                />
              </InlineField>
              <Combobox<string>
                value={f.column || null}
                placeholder="column"
                options={loadColumns}
                createCustomValue
                width={20}
                aria-label={`Filter ${i + 1} column`}
                onChange={(o) => {
                  const k = filterKind(o.value);
                  updateFilter(i, {
                    column: o.value,
                    valueType: valueTypeFor(k),
                    op: operatorsFor(k).includes(f.op) ? f.op : '=',
                  });
                }}
              />
              <Combobox<FilterOp>
                value={f.op}
                options={operatorsFor(filterKind(f.column)).map((op) => ({ label: op, value: op }))}
                width={12}
                aria-label={`Filter ${i + 1} operator`}
                onChange={(o) => updateFilter(i, { op: o.value })}
              />
              {f.op !== 'IS NULL' && f.op !== 'IS NOT NULL' && (
                <Input
                  defaultValue={f.value ?? ''}
                  placeholder="value or $variable"
                  width={24}
                  aria-label={`Filter ${i + 1} value`}
                  onBlur={(e) => updateFilter(i, { value: e.currentTarget.value })}
                />
              )}
              <IconButton name="times" aria-label={`Remove filter ${i + 1}`} onClick={() => removeFilter(i)} />
            </div>
          ))}
          <div className={styles.indent}>
            <Button
              size="sm"
              variant="secondary"
              icon="plus"
              onClick={() => commit({ ...b, where: [...b.where, { column: '', op: '=', valueType: 'string' }] })}
            >
              Add filter
            </Button>
          </div>
          <InlineField label="Limit" labelWidth={16}>
            <Input
              defaultValue={b.limit ?? ''}
              placeholder="1000"
              width={12}
              aria-label="Limit"
              onBlur={(e) => commit({ ...b, limit: e.currentTarget.value.trim() || undefined })}
            />
          </InlineField>
        </>
      )}
      {result.query !== undefined ? (
        <pre className={styles.sql} data-testid="arc-variable-sql">
          {b.kind === 'columns' ? `Columns of ${b.measurement} from Arc's schema` : result.query}
        </pre>
      ) : (
        <ul className={styles.errors} data-testid="arc-variable-errors">
          {result.errors.map((e, i) => (
            <li key={i}>{e.message}</li>
          ))}
        </ul>
      )}
      {b.kind === 'values' && (
        <div className={styles.hint}>
          Values come from the dashboard&apos;s time range. Set Refresh to &quot;On time range change&quot; to follow
          it; otherwise they are read once, when the dashboard loads.
        </div>
      )}
    </div>
  );
}

function VariableCodeEditor({ query, onChange }: { query: string; onChange: (text: string) => void }) {
  const styles = useStyles2(getStyles);
  const [state, setState] = useState(query);

  // Sync local state when the parent's SQL changes (another variable picked,
  // the dashboard JSON edited), comparing content rather than the object:
  // the parent re-renders with fresh objects on every refresh, and resetting
  // then would overwrite unsaved typing. (R1 H6.) Adjust-state-during-render
  // rather than an effect, so it is lint-clean under set-state-in-effect.
  const [synced, setSynced] = useState(query);
  if (query !== synced) {
    setSynced(query);
    setState(query);
  }

  return (
    <>
      <InlineField
        label="Query"
        labelWidth={20}
        tooltip="SQL query to generate variable values. Values come from the first text column; SHOW TABLES uses table_name."
        grow
      >
        <TextArea
          className={styles.textarea}
          value={state}
          onChange={(e) => setState(e.currentTarget.value)}
          onBlur={() => onChange(state)}
          placeholder="SELECT DISTINCT host FROM telegraf.cpu ORDER BY host"
          rows={3}
        />
      </InlineField>

      <div className={styles.examples}>
        <small>
          <strong>Examples:</strong>
          <br />• Get distinct hosts:{' '}
          <code className={styles.code}>SELECT DISTINCT host FROM telegraf.cpu ORDER BY host</code>
          <br />• Get tables: <code className={styles.code}>SHOW TABLES</code>
          <br />• Get databases: <code className={styles.code}>SHOW DATABASES</code>
        </small>
      </div>
    </>
  );
}

const getStyles = (theme: GrafanaTheme2) => ({
  toolbar: css({ marginBottom: theme.spacing(1) }),
  rows: css({ display: 'flex', flexDirection: 'column', gap: theme.spacing(0.5) }),
  filter: css({ display: 'flex', alignItems: 'center', gap: theme.spacing(0.5) }),
  indent: css({ marginLeft: theme.spacing(16.5) }),
  sql: css({
    fontFamily: theme.typography.fontFamilyMonospace,
    fontSize: theme.typography.bodySmall.fontSize,
    whiteSpace: 'pre-wrap',
    margin: theme.spacing(1, 0, 0),
    padding: theme.spacing(1),
    background: theme.colors.background.secondary,
    borderRadius: theme.shape.radius.default,
  }),
  errors: css({
    color: theme.colors.warning.text,
    fontSize: theme.typography.bodySmall.fontSize,
    margin: theme.spacing(0.5, 0, 0, 2),
  }),
  hint: css({ fontSize: theme.typography.bodySmall.fontSize, color: theme.colors.text.secondary }),
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
