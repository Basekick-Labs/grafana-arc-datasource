/**
 * The variable query builder (iteration 2, plan v2 §3.5).
 *
 * A variable query is `{ query, editorMode?, builder? }`. As with panels, the
 * builder state counts only while `builder.sql === query`; anything else is
 * Code. Databases, measurements and columns come from the plugin's metadata
 * routes (scoped-key fallback, any protocol); values are SQL, sent as
 * builder SQL so a chained variable interpolates as a quoted literal.
 */
import { Filter } from '../types';
import { FieldError, MEASUREMENT_RE, quoteIdent, variableName, whereSQL } from './generate';

export type VariableKind = 'databases' | 'measurements' | 'columns' | 'values';

export const VARIABLE_BUILDER_VERSION = 1;

export interface VariableBuilder {
  version: number;
  kind: VariableKind;
  database?: string;
  measurement?: string;
  /** values: the column whose distinct values are listed. */
  column?: string;
  /** columns: text and boolean columns only (tag keys), the default. */
  textOnly?: boolean;
  /** values: filters, as in the panel builder. */
  where: Filter[];
  /** values: integer lexeme; default 1000. */
  limit?: string;
  /** Fingerprint: exactly the query generateVariable() committed. */
  sql: string;
}

export interface ArcVariableQuery {
  query: string;
  editorMode?: 'builder' | 'code';
  builder?: VariableBuilder;
}

export function emptyVariableBuilder(): VariableBuilder {
  return { version: VARIABLE_BUILDER_VERSION, kind: 'values', where: [], sql: '' };
}

/** Normalises what Grafana stores for a variable: a string (old), `{query}`, or ours. */
export function toVariableQuery(q: unknown): ArcVariableQuery {
  if (typeof q === 'string') {
    return { query: q };
  }
  if (q && typeof q === 'object') {
    const o = q as { query?: unknown; sql?: unknown; rawSql?: unknown };
    const query =
      typeof o.query === 'string'
        ? o.query
        : typeof o.sql === 'string'
          ? o.sql
          : typeof o.rawSql === 'string'
            ? o.rawSql
            : '';
    return { ...(q as ArcVariableQuery), query };
  }
  return { query: '' };
}

/**
 * Builder only for a query the builder wrote and still matches, or a new,
 * empty one. Everything saved before the builder existed stays Code.
 */
export function resolveVariableMode(q: ArcVariableQuery): 'builder' | 'code' {
  const b = q.builder;
  if (q.editorMode === undefined && !b && !q.query.trim()) {
    return 'builder';
  }
  if (q.editorMode !== 'builder' || !b || b.version !== VARIABLE_BUILDER_VERSION) {
    return 'code';
  }
  return b.sql === q.query ? 'builder' : 'code';
}

function nameError(field: string, value: string | undefined, re: RegExp, what: string): FieldError | undefined {
  const v = (value ?? '').trim();
  if (!v || variableName(v) !== undefined || re.test(v)) {
    return undefined;
  }
  return { field, message: `"${v}" is not a ${what} name Arc accepts` };
}

const DATABASE_RE = /^[A-Za-z0-9_-]+$/;

/**
 * The query text for a variable builder state, or why there is none.
 * Databases and measurements store the equivalent SHOW statement, so an
 * older plugin still lists something; columns have no SQL form, and store
 * a comment.
 */
export function generateVariable(
  b: VariableBuilder
): { query: string; errors?: undefined } | { query?: undefined; errors: FieldError[] } {
  const errors: FieldError[] = [];
  const dbErr = nameError('database', b.database, DATABASE_RE, 'database');
  if (dbErr) {
    errors.push(dbErr);
  }
  const db = (b.database ?? '').trim();
  const m = (b.measurement ?? '').trim();
  if (b.kind === 'columns' || b.kind === 'values') {
    if (!m) {
      errors.push({ field: 'measurement', message: 'Pick a measurement' });
    } else {
      const e = nameError('measurement', m, MEASUREMENT_RE, 'measurement');
      if (e) {
        errors.push(e);
      }
    }
  }
  if (b.kind === 'values') {
    if (!b.column) {
      errors.push({ field: 'column', message: 'Pick a column' });
    }
    const limit = (b.limit ?? '').trim();
    if (limit && !/^[1-9]\d{0,5}$/.test(limit)) {
      errors.push({ field: 'limit', message: 'LIMIT must be a whole number from 1 to 999999' });
    }
    b.where.forEach((f, i) => {
      if (!f.column) {
        errors.push({ field: `where.${i}.column`, message: 'Pick a column' });
      }
    });
  }
  if (errors.length) {
    return { errors };
  }
  switch (b.kind) {
    case 'databases':
      return { query: 'SHOW DATABASES' };
    case 'measurements':
      return { query: db ? `SHOW TABLES FROM ${quoteIdent(db)}` : 'SHOW TABLES' };
    case 'columns':
      return {
        query: `-- Arc builder: ${b.textOnly === false ? 'columns' : 'text columns'} of ${m}${db ? ` in ${db}` : ''}`,
      };
    case 'values': {
      const from = variableName(m) !== undefined ? quoteIdent('${' + variableName(m) + ':raw}') : quoteIdent(m);
      const c = quoteIdent(b.column!);
      return {
        query: `SELECT DISTINCT ${c} AS "value"\nFROM ${from}\nWHERE ${whereSQL(b.where)} AND ${c} IS NOT NULL\nORDER BY 1\nLIMIT ${(b.limit ?? '').trim() || '1000'}`,
      };
    }
  }
}

/** The short text Grafana lists beside the variable. */
export function variableDefinition(b: VariableBuilder): string {
  switch (b.kind) {
    case 'databases':
      return 'Arc databases';
    case 'measurements':
      return `Arc measurements${b.database ? ` in ${b.database}` : ''}`;
    case 'columns':
      return `Arc ${b.textOnly === false ? 'columns' : 'tag keys'} of ${b.measurement ?? '?'}`;
    case 'values':
      return `Arc values of ${b.column ?? '?'} in ${b.measurement ?? '?'}${b.where.length ? ' (filtered)' : ''}`;
  }
}
