/**
 * Which editor a query opens in (issue #20, plan v3 §4.2).
 *
 * `editorMode` alone cannot be trusted: Grafana merges the datasource's
 * getDefaultQuery() over existing queries when it opens an alert rule
 * (QueryWrapper.tsx) and when a panel switches datasource
 * (QueryEditorRows.tsx). Every legacy query would then claim to be an empty
 * builder query. The fingerprint, the SQL the builder last committed, is what
 * proves the saved SQL is the builder's.
 */
import { ArcQuery, EditorMode, GENERATOR_VERSION } from '../types';

const KNOWN_VERSION = 1;
const KNOWN_GENERATOR = GENERATOR_VERSION;

export function resolveMode(query: Partial<ArcQuery>): EditorMode {
  const b = query.builder;
  if (query.editorMode === undefined && !b && !(query.sql ?? '').trim() && !query.rawSql) {
    // A brand-new query that Grafana created without getDefaultQuery (a new
    // panel on Grafana 12.3 is just {refId}). Nothing to preserve: a saved
    // query always has SQL.
    return 'builder';
  }
  if (query.editorMode !== 'builder' || !b) {
    return 'code';
  }
  if (b.version !== KNOWN_VERSION || typeof b.generatorVersion !== 'number' || b.generatorVersion > KNOWN_GENERATOR) {
    return 'code';
  }
  const sql = query.sql ?? '';
  if (sql === b.sql) {
    // An empty builder over a query migrated from another SQL datasource
    // (rawSql) is the datasource-switch case, not a new builder query.
    return sql === '' && query.rawSql ? 'code' : 'builder';
  }
  return 'code';
}

/**
 * True when the builder state is only the defaults Grafana merged in: an
 * empty builder that was never committed. Code mode drops it instead of
 * persisting it on the first edit.
 */
export function isPristineDefault(query: Partial<ArcQuery>): boolean {
  const b = query.builder;
  return (
    query.editorMode === 'builder' &&
    !!b &&
    b.sql === '' &&
    !b.measurement &&
    b.select.length === 0 &&
    b.where.length === 0 &&
    b.groupBy.tags.length === 0
  );
}

/**
 * The query to persist after an edit made in Code mode: the patch applied,
 * with merged-in builder defaults removed so they are never saved.
 */
export function codeModeChange(query: ArcQuery, patch: Partial<ArcQuery>): ArcQuery {
  const next: ArcQuery = { ...query, ...patch };
  if (isPristineDefault(query)) {
    delete next.editorMode;
    delete next.builder;
  }
  return next;
}
