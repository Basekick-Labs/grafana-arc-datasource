/**
 * The visual query builder (issue #20): InfluxQL-style rows that generate
 * Arc SQL. Every change regenerates `sql` and the `builder.sql` fingerprint
 * together; an incomplete query saves `sql: ''`, which filterQuery skips.
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { CoreApp, GrafanaTheme2, SelectableValue, TimeRange } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import {
  Alert,
  Button,
  Icon,
  IconButton,
  InlineLabel,
  InlineSegmentGroup,
  Segment,
  SegmentAsync,
  SegmentInput,
  useStyles2,
} from '@grafana/ui';
import { css } from '@emotion/css';
import { ArcDataSource, ColumnInfo } from '../datasource';
import {
  AggregateFn,
  ArcQuery,
  BuilderQuery,
  Filter,
  FilterOp,
  GENERATOR_VERSION,
  SelectItem,
  TransformKind,
  emptyBuilder,
} from '../types';
import { FieldError, defaultAlias, generate, isAggregate, variableName } from './generate';
import {
  ColumnKind,
  FUNCTIONS,
  TRANSFORMS,
  defaultFunctionFor,
  functionIneligibility,
  transformIneligibility,
  kindOf,
  operatorsFor,
  valueTypeFor,
} from './columns';
import { VariableInfo, variableErrors } from './variables';
import { rawSingle } from './interpolate';

export interface BuilderEditorProps {
  query: ArcQuery;
  datasource: ArcDataSource;
  onChange: (q: ArcQuery) => void;
  onRunQuery: () => void;
  onSwitchToCode: () => void;
  range?: TimeRange;
  app?: CoreApp;
}

const INTERVALS = ['$__interval', '1s', '10s', '30s', '1m', '5m', '10m', '15m', '30m', '1h', '6h', '12h', '1d'];
/** A "+" segment trigger. The label sits on a wrapper: Grafana 13.3's Icon no longer passes aria-label through. */
const addButton = (label: string) => (
  <span aria-label={label} title={label} role="button">
    <Icon name="plus" />
  </span>
);

type ColumnsState =
  | { status: 'idle' | 'loading' }
  | { status: 'loaded'; columns: ColumnInfo[]; source: string; message?: string }
  | { status: 'error'; message: string };

/** Commits builder state: regenerates sql and the fingerprint together. */
export function commitBuilder(
  query: ArcQuery,
  b: BuilderQuery,
  extraErrors: FieldError[] = []
): { query: ArcQuery; valid: boolean } {
  const result = generate(b, query.format ?? 'time_series');
  const sql = result.sql !== undefined && extraErrors.length === 0 ? result.sql : '';
  return {
    query: {
      ...query,
      editorMode: 'builder',
      builder: { ...b, version: 1, generatorVersion: GENERATOR_VERSION, sql },
      sql,
    },
    valid: sql !== '',
  };
}

function templateVariables(): VariableInfo[] {
  try {
    return getTemplateSrv()
      .getVariables()
      .map((v) => v as unknown as VariableInfo);
  } catch {
    return [];
  }
}

/** Resolves a value that may be a single-value variable to the name to look up metadata for. */
function resolveForLookup(raw: string | undefined): { value?: string; error?: string } {
  if (!raw) {
    return {};
  }
  if (!raw.includes('$')) {
    return { value: raw };
  }
  try {
    return { value: getTemplateSrv().replace(raw, undefined, rawSingle) };
  } catch (e) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

function errorMessage(e: unknown): string {
  const data = (e as { data?: { error?: string } })?.data;
  if (data?.error) {
    return data.error;
  }
  return e instanceof Error ? e.message : String(e);
}

export function BuilderEditor({
  query,
  datasource,
  onChange,
  onRunQuery,
  onSwitchToCode,
  range,
  app,
}: BuilderEditorProps) {
  const styles = useStyles2(getStyles);
  const b = query.builder ?? emptyBuilder();
  const format = query.format ?? 'time_series';
  const inAlerting = app === CoreApp.UnifiedAlerting;
  const variables = useMemo(() => (inAlerting ? [] : templateVariables()), [inAlerting]);
  const varOptions = useMemo(
    () => variables.map((v) => ({ label: '$' + v.name, value: '$' + v.name, description: 'Dashboard variable' })),
    [variables]
  );
  const singleVarOptions = useMemo(
    () =>
      variables
        .filter((v) => !v.multi && !v.includeAll)
        .map((v) => ({ label: '$' + v.name, value: '$' + v.name, description: 'Dashboard variable' })),
    [variables]
  );
  const [loaded, setLoaded] = useState<{ key: string; state: ColumnsState }>({ key: '', state: { status: 'idle' } });
  const [notice, setNotice] = useState<string>();

  const lookupDb = resolveForLookup(query.database);
  const lookupM = resolveForLookup(b.measurement);
  const database = lookupDb.value ?? '';

  // Column metadata for the chosen measurement. The effect only stores
  // results; idle and loading are derived from whether the stored result is
  // for the current database and measurement.
  const columnsKey = lookupM.value && !lookupDb.error && !lookupM.error ? `${database}\u0000${lookupM.value}` : '';
  useEffect(() => {
    if (!columnsKey) {
      return;
    }
    let cancelled = false;
    datasource
      .getColumns(database, lookupM.value ?? '')
      .then(
        (res) =>
          !cancelled &&
          setLoaded({
            key: columnsKey,
            state: { status: 'loaded', columns: res.columns ?? [], source: res.source, message: res.message },
          })
      )
      .catch((e) => !cancelled && setLoaded({ key: columnsKey, state: { status: 'error', message: errorMessage(e) } }));
    return () => {
      cancelled = true;
    };
  }, [datasource, database, lookupM.value, columnsKey]);
  const columns: ColumnsState = useMemo(
    () => (!columnsKey ? { status: 'idle' } : loaded.key === columnsKey ? loaded.state : { status: 'loading' }),
    [columnsKey, loaded]
  );

  const kinds = useMemo(() => {
    const map = new Map<string, ColumnKind>();
    if (columns.status === 'loaded') {
      columns.columns.forEach((c) => map.set(c.name, kindOf(c.type)));
    }
    return map;
  }, [columns]);
  const kindOfColumn = useCallback((name: string): ColumnKind => kinds.get(name) ?? 'unknown', [kinds]);

  const varErrors = variableErrors(b, query.database, variables, inAlerting);
  const result = generate(b, format);
  const errors = [...(result.errors ?? []), ...varErrors];

  const commit = useCallback(
    (next: BuilderQuery, patch: Partial<ArcQuery> = {}) => {
      const base = { ...query, ...patch };
      const extra = variableErrors(next, base.database, variables, inAlerting);
      const { query: q, valid } = commitBuilder(base, next, extra);
      onChange(q);
      if (valid) {
        onRunQuery();
      }
    },
    [query, onChange, onRunQuery, variables, inAlerting]
  );

  // ---- option loaders ------------------------------------------------------

  const loadDatabases = async (): Promise<Array<SelectableValue<string>>> => {
    const res = await datasource.getDatabases();
    const opts: Array<SelectableValue<string>> = res.databases.map((d) => ({ label: d, value: d }));
    if (res.scoped) {
      opts.push({
        label: 'Type a name…',
        value: '',
        description: 'This API key cannot list every database',
        isDisabled: true,
      });
    }
    return [...singleVarOptions, ...opts];
  };

  const loadMeasurements = async (): Promise<Array<SelectableValue<string>>> => {
    if (lookupDb.error) {
      throw new Error(lookupDb.error);
    }
    const names = await datasource.getMeasurements(database);
    return [...singleVarOptions, ...names.map((m) => ({ label: m, value: m }))];
  };

  const columnOptions = (purpose: 'filter' | 'group' | 'select'): Array<SelectableValue<string>> => {
    if (columns.status !== 'loaded') {
      return [];
    }
    const cols = columns.columns.filter((c) => c.name !== 'time');
    const rank = (k: ColumnKind) =>
      purpose === 'select' ? (k === 'number' ? 0 : k === 'other' ? 2 : 1) : k === 'string' ? 0 : k === 'other' ? 2 : 1;
    return [...cols]
      .sort((x, y) => rank(kindOf(x.type)) - rank(kindOf(y.type)) || x.name.localeCompare(y.name))
      .map((c) => {
        const k = kindOf(c.type);
        const disabled = (purpose !== 'select' && k === 'other') || (purpose === 'group' && k === 'number');
        return {
          label: c.name,
          value: c.name,
          description: c.type + (columns.source === 'describe' ? ' (inferred from recent data)' : ''),
          isDisabled: disabled,
        };
      });
  };

  // Value suggestions: debounced, the latest request wins.
  const latestLookup = useRef(0);
  const loadValues =
    (i: number) =>
    async (search?: string): Promise<Array<SelectableValue<string>>> => {
      const id = ++latestLookup.current;
      await new Promise((r) => setTimeout(r, search ? 300 : 0));
      if (id !== latestLookup.current) {
        return [];
      }
      const f = b.where[i];
      const others = b.where.filter((_, j) => j !== i);
      const opts: Array<SelectableValue<string>> = [...varOptions];
      if (kindOfColumn(f.column) === 'string' || kindOfColumn(f.column) === 'unknown') {
        const { values, more } = await datasource.getValues(b, database, f.column, others, search ?? '', range);
        opts.push(...values.map((v) => ({ label: v === '' ? '(empty)' : v, value: v })));
        if (more) {
          opts.push({ label: 'More values: type to search', value: '__more__', isDisabled: true });
        }
      } else if (kindOfColumn(f.column) === 'boolean') {
        opts.push({ label: 'true', value: 'true' }, { label: 'false', value: 'false' });
      }
      return opts;
    };

  // ---- mutations -------------------------------------------------------------

  const setMeasurement = async (m: string) => {
    const next: BuilderQuery = { ...b, measurement: m };
    const resolved = resolveForLookup(m);
    let cols: ColumnInfo[] | undefined;
    if (resolved.value) {
      try {
        cols = (await datasource.getColumns(database, resolved.value)).columns;
      } catch {
        cols = undefined;
      }
    }
    if (cols && cols.length > 0) {
      const names = new Set(cols.map((c) => c.name));
      const dropped = [
        ...b.select.filter((s) => s.fn !== 'count_rows' && !names.has(s.column)).map((s) => s.column),
        ...b.where.filter((f) => !names.has(f.column)).map((f) => f.column),
        ...b.groupBy.tags.filter((t) => !names.has(t)),
      ];
      next.select = b.select.filter((s) => s.fn === 'count_rows' || names.has(s.column));
      next.where = b.where.filter((f) => names.has(f.column));
      next.groupBy = { ...b.groupBy, tags: b.groupBy.tags.filter((t) => names.has(t)) };
      setNotice(dropped.length ? `Not in ${m}, removed: ${Array.from(new Set(dropped)).join(', ')}` : undefined);
      if (next.select.length === 0) {
        // InfluxQL starts a new query with mean() of the first field.
        const first = cols.find((c) => kindOf(c.type) === 'number');
        if (first) {
          next.select = [{ column: first.name, fn: 'mean' }];
        }
      }
    } else {
      setNotice(undefined);
    }
    commit(next);
  };

  const setDatabase = (db: string) => {
    setNotice(undefined);
    commit(
      { ...b, measurement: undefined, select: [], where: [], groupBy: { ...b.groupBy, tags: [] } },
      { database: db }
    );
  };

  const updateSelect = (i: number, patch: Partial<SelectItem>) =>
    commit({ ...b, select: b.select.map((s, j) => (j === i ? { ...s, ...patch } : s)) });
  const removeSelect = (i: number) => commit({ ...b, select: b.select.filter((_, j) => j !== i) });
  const addSelect = (column: string) => {
    const allRaw = b.select.length > 0 && b.select.every((s) => s.fn === 'raw');
    const fn: AggregateFn = allRaw ? 'raw' : defaultFunctionFor(kindOfColumn(column));
    commit({ ...b, select: [...b.select, { column, fn }] });
  };

  const updateFilter = (i: number, patch: Partial<Filter>) =>
    commit({ ...b, where: b.where.map((f, j) => (j === i ? { ...f, ...patch } : f)) });
  const removeFilter = (i: number) => commit({ ...b, where: b.where.filter((_, j) => j !== i) });
  const addFilter = (column: string) => {
    const kind = kindOfColumn(column);
    commit({ ...b, where: [...b.where, { column, op: '=', valueType: valueTypeFor(kind) }] });
  };

  // ---- render ----------------------------------------------------------------

  const aggregated = b.select.some((s) => isAggregate(s.fn));
  const dbHidden = !datasource.allowsDatabaseOverride();
  // Transformations run over values in time order: buckets, or a raw time series.
  const noTransformWhy = (aggregated ? !b.groupBy.time : format === 'table')
    ? 'needs GROUP BY time, or a raw time series'
    : undefined;

  return (
    <div className={styles.rows} data-testid="arc-query-builder">
      <InlineSegmentGroup>
        <InlineLabel width={10} className={styles.keyword}>
          FROM
        </InlineLabel>
        {!dbHidden && (
          <SegmentAsync
            value={query.database || undefined}
            placeholder={datasource.defaultDatabase()}
            loadOptions={loadDatabases}
            onChange={(o) => setDatabase(o.value ?? '')}
            allowCustomValue
            inputMinWidth={160}
            noOptionMessageHandler={(s) => (s.error ? errorMessage(s.error) : 'No databases')}
          />
        )}
        <SegmentAsync
          value={b.measurement}
          placeholder="select measurement"
          loadOptions={loadMeasurements}
          onChange={(o) => setMeasurement(o.value ?? '')}
          allowCustomValue
          inputMinWidth={160}
          noOptionMessageHandler={(s) => (s.error ? errorMessage(s.error) : 'No measurements')}
        />
        <InlineLabel width="auto" className={styles.keyword}>
          WHERE
        </InlineLabel>
        {b.where.map((f, i) => {
          const kind = kindOfColumn(f.column);
          const valueless = f.op === 'IS NULL' || f.op === 'IS NOT NULL';
          return (
            <React.Fragment key={i}>
              {i > 0 && <InlineLabel width="auto">AND</InlineLabel>}
              <Segment
                value={f.column}
                options={columnOptions('filter')}
                allowCustomValue
                onChange={(o) => {
                  const k = kindOfColumn(o.value ?? '');
                  updateFilter(i, {
                    column: o.value ?? '',
                    valueType: valueTypeFor(k),
                    op: operatorsFor(k).includes(f.op) ? f.op : '=',
                  });
                }}
              />
              <Segment<FilterOp>
                value={f.op}
                options={operatorsFor(kind).map((op) => ({ label: op, value: op }))}
                onChange={(o) =>
                  updateFilter(i, {
                    op: o.value ?? '=',
                    value: o.value === 'IS NULL' || o.value === 'IS NOT NULL' ? undefined : f.value,
                  })
                }
              />
              {!valueless && (
                <SegmentAsync
                  value={f.value}
                  placeholder="value"
                  loadOptions={loadValues(i)}
                  reloadOptionsOnChange
                  onChange={(o) => updateFilter(i, { value: o.value ?? '' })}
                  allowCustomValue
                  inputMinWidth={160}
                  noOptionMessageHandler={(s) => (s.error ? errorMessage(s.error) : 'No values in this time range')}
                />
              )}
              <IconButton
                name="times"
                size="sm"
                aria-label={`Remove filter ${i + 1}`}
                onClick={() => removeFilter(i)}
              />
            </React.Fragment>
          );
        })}
        <Segment
          Component={addButton('Add filter')}
          options={columnOptions('filter')}
          allowCustomValue
          onChange={(o) => o.value && addFilter(o.value)}
        />
      </InlineSegmentGroup>

      <InlineSegmentGroup>
        <InlineLabel width={10} className={styles.keyword}>
          SELECT
        </InlineLabel>
        {b.select.map((s, i) => {
          const kind = kindOfColumn(s.column);
          return (
            <React.Fragment key={i}>
              {i > 0 && <InlineLabel width="auto">,</InlineLabel>}
              <Segment<AggregateFn>
                value={s.fn === 'count_rows' ? 'count' : s.fn}
                options={FUNCTIONS.map((o) => {
                  const why = functionIneligibility(o.fn, kind, format);
                  return {
                    label: o.label,
                    value: o.fn,
                    description: why ? `${o.description}: ${why}` : o.description,
                    isDisabled: !!why,
                  };
                })}
                onChange={(o) =>
                  updateSelect(i, {
                    fn: o.value ?? 'mean',
                    param: o.value === 'percentile' ? (s.param ?? '95') : undefined,
                  })
                }
              />
              {s.fn === 'percentile' && (
                <SegmentInput
                  value={s.param ?? ''}
                  placeholder="95"
                  onChange={(v) => updateSelect(i, { param: String(v) })}
                />
              )}
              {s.fn !== 'count_rows' && (
                <Segment
                  value={s.column}
                  options={columnOptions('select')}
                  allowCustomValue
                  onChange={(o) => updateSelect(i, { column: o.value ?? '' })}
                />
              )}
              <Segment<TransformKind | ''>
                value={s.transform?.kind}
                placeholder="transform"
                aria-label={`Transformation for field ${i + 1}`}
                options={[
                  { label: 'none', value: '', description: 'No transformation' },
                  ...TRANSFORMS.map((o) => {
                    const why = noTransformWhy ?? transformIneligibility(s.fn, kind);
                    return {
                      label: o.label,
                      value: o.kind,
                      description: why ? `${o.description}: ${why}` : o.description,
                      isDisabled: !!why,
                    };
                  }),
                ]}
                onChange={(o) =>
                  updateSelect(i, {
                    transform: o.value
                      ? { kind: o.value, n: o.value === 'moving_average' ? (s.transform?.n ?? '10') : undefined }
                      : undefined,
                  })
                }
              />
              {(s.transform?.kind === 'derivative' || s.transform?.kind === 'non_negative_derivative') && (
                <SegmentInput
                  value={s.transform.unit ?? ''}
                  placeholder="1s"
                  aria-label={`Unit for field ${i + 1}`}
                  onChange={(v) =>
                    updateSelect(i, { transform: { ...s.transform!, unit: String(v).trim() || undefined } })
                  }
                />
              )}
              {s.transform?.kind === 'moving_average' && (
                <SegmentInput
                  value={s.transform.n ?? ''}
                  placeholder="10"
                  aria-label={`Window for field ${i + 1}`}
                  onChange={(v) => updateSelect(i, { transform: { ...s.transform!, n: String(v).trim() } })}
                />
              )}
              <SegmentInput
                value={s.math ?? ''}
                placeholder="math"
                inputPlaceholder="* 8 / 1024"
                aria-label={`Math for field ${i + 1}`}
                onChange={(v) => updateSelect(i, { math: String(v).trim() || undefined })}
              />
              <InlineLabel width="auto">AS</InlineLabel>
              <SegmentInput
                value={s.alias ?? ''}
                placeholder={s.transform?.kind ?? (defaultAlias(s) || 'alias')}
                onChange={(v) => updateSelect(i, { alias: String(v).trim() || undefined })}
              />
              <IconButton name="times" size="sm" aria-label={`Remove field ${i + 1}`} onClick={() => removeSelect(i)} />
            </React.Fragment>
          );
        })}
        <Segment
          Component={addButton('Add field')}
          options={[{ label: 'count rows', value: '*', description: 'count(*)' }, ...columnOptions('select')]}
          allowCustomValue
          onChange={(o) => {
            if (o.value === '*') {
              commit({ ...b, select: [...b.select, { column: '', fn: 'count_rows' }] });
            } else if (o.value) {
              addSelect(o.value);
            }
          }}
        />
      </InlineSegmentGroup>

      {(aggregated || b.groupBy.tags.length > 0) && (
        <InlineSegmentGroup>
          <InlineLabel width={10} className={styles.keyword}>
            GROUP BY
          </InlineLabel>
          {aggregated &&
            (b.groupBy.time ? (
              <>
                <InlineLabel width="auto">time(</InlineLabel>
                <Segment
                  value={b.groupBy.time.interval}
                  options={INTERVALS.map((v) => ({ label: v, value: v }))}
                  allowCustomValue
                  onChange={(o) =>
                    commit({ ...b, groupBy: { ...b.groupBy, time: { interval: o.value ?? '$__interval' } } })
                  }
                />
                <InlineLabel width="auto">)</InlineLabel>
                {format === 'table' && (
                  <IconButton
                    name="times"
                    size="sm"
                    aria-label="Remove GROUP BY time"
                    onClick={() => commit({ ...b, groupBy: { ...b.groupBy, time: undefined } })}
                  />
                )}
              </>
            ) : (
              <Button
                size="sm"
                variant="secondary"
                fill="text"
                onClick={() => commit({ ...b, groupBy: { ...b.groupBy, time: { interval: '$__interval' } } })}
              >
                time()
              </Button>
            ))}
          {b.groupBy.tags.map((t, i) => (
            <React.Fragment key={i}>
              <Segment
                value={t}
                options={columnOptions('group')}
                allowCustomValue
                onChange={(o) =>
                  commit({
                    ...b,
                    groupBy: { ...b.groupBy, tags: b.groupBy.tags.map((x, j) => (j === i ? (o.value ?? '') : x)) },
                  })
                }
              />
              <IconButton
                name="times"
                size="sm"
                aria-label={`Remove GROUP BY ${t}`}
                onClick={() =>
                  commit({ ...b, groupBy: { ...b.groupBy, tags: b.groupBy.tags.filter((_, j) => j !== i) } })
                }
              />
            </React.Fragment>
          ))}
          <Segment
            Component={addButton('Add GROUP BY')}
            options={columnOptions('group')}
            allowCustomValue
            onChange={(o) => o.value && commit({ ...b, groupBy: { ...b.groupBy, tags: [...b.groupBy.tags, o.value] } })}
          />
        </InlineSegmentGroup>
      )}
      {!aggregated && b.groupBy.tags.length === 0 && b.select.length > 0 && (
        <InlineSegmentGroup>
          <InlineLabel width={10} className={styles.keyword}>
            GROUP BY
          </InlineLabel>
          <Segment
            Component={addButton('Add GROUP BY')}
            options={columnOptions('group')}
            allowCustomValue
            onChange={(o) => o.value && commit({ ...b, groupBy: { ...b.groupBy, tags: [o.value] } })}
          />
        </InlineSegmentGroup>
      )}

      <InlineSegmentGroup>
        {format === 'table' && (aggregated ? !!b.groupBy.time : true) && (
          <>
            <InlineLabel width={10} className={styles.keyword}>
              ORDER BY
            </InlineLabel>
            <InlineLabel width="auto">time</InlineLabel>
            <Segment<'ASC' | 'DESC'>
              value={b.orderByTime ?? (aggregated ? 'ASC' : 'DESC')}
              options={[
                { label: 'ASC', value: 'ASC' },
                { label: 'DESC', value: 'DESC' },
              ]}
              onChange={(o) => commit({ ...b, orderByTime: o.value })}
            />
          </>
        )}
        <InlineLabel width={10} className={styles.keyword} tooltip="A LIMIT turns off query splitting for this query">
          LIMIT
        </InlineLabel>
        <SegmentInput
          value={b.limit ?? ''}
          placeholder={!aggregated && format === 'table' ? '1000' : 'none'}
          onChange={(v) => commit({ ...b, limit: String(v).trim() || undefined })}
        />
      </InlineSegmentGroup>

      {columns.status === 'error' && (
        <Alert severity="warning" title="Could not load columns">
          {columns.message} You can still type column names.
        </Alert>
      )}
      {columns.status === 'loaded' && columns.source === 'none' && (
        <Alert severity="info" title={columns.message ?? 'No columns found'} />
      )}
      {(lookupDb.error || lookupM.error) && <Alert severity="error" title={lookupDb.error ?? lookupM.error ?? ''} />}
      {notice && <Alert severity="info" title={notice} onRemove={() => setNotice(undefined)} />}

      <div className={styles.preview}>
        <div className={styles.previewHeader}>
          <span>SQL</span>
          <Button size="sm" variant="secondary" fill="text" icon="pen" onClick={onSwitchToCode}>
            Edit as SQL
          </Button>
        </div>
        {result.sql !== undefined && varErrors.length === 0 ? (
          <pre className={styles.sql} data-testid="arc-builder-sql">
            {result.sql}
          </pre>
        ) : (
          <ul className={styles.errors} data-testid="arc-builder-errors">
            {errors.map((e, i) => (
              <li key={i}>{e.message}</li>
            ))}
          </ul>
        )}
        {b.where.some((f) => (f.value ?? '').includes('$') && variableName(f.value) === undefined) && (
          <div className={styles.hint}>A value containing $ is substituted by Grafana like a variable.</div>
        )}
      </div>
    </div>
  );
}

const getStyles = (theme: GrafanaTheme2) => ({
  rows: css({ display: 'flex', flexDirection: 'column', gap: theme.spacing(0.5) }),
  keyword: css({ color: theme.colors.primary.text, fontWeight: theme.typography.fontWeightMedium }),
  preview: css({ marginTop: theme.spacing(1) }),
  previewHeader: css({
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    fontSize: theme.typography.bodySmall.fontSize,
    color: theme.colors.text.secondary,
  }),
  sql: css({
    fontFamily: theme.typography.fontFamilyMonospace,
    fontSize: theme.typography.bodySmall.fontSize,
    whiteSpace: 'pre-wrap',
    margin: 0,
    padding: theme.spacing(1),
    background: theme.colors.background.secondary,
    borderRadius: theme.shape.radius.default,
  }),
  errors: css({
    color: theme.colors.warning.text,
    fontSize: theme.typography.bodySmall.fontSize,
    margin: theme.spacing(0.5, 0, 0, 2),
  }),
  hint: css({
    fontSize: theme.typography.bodySmall.fontSize,
    color: theme.colors.text.secondary,
    marginTop: theme.spacing(0.5),
  }),
});
