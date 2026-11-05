import { ArcQuery, builderDefaultQuery, emptyBuilder } from '../types';
import { codeModeChange, isPristineDefault, resolveMode } from './mode';

const committed = 'SELECT time, "v"\nFROM "cpu"\nWHERE $__timeFilter(time)\nORDER BY time ASC';
const builderQuery = (): ArcQuery => ({
  refId: 'A',
  editorMode: 'builder',
  sql: committed,
  builder: { ...emptyBuilder(), measurement: 'cpu', select: [{ column: 'v', fn: 'raw' }], sql: committed },
});

// The cases come from the plan v3 review (B1): Grafana merges
// getDefaultQuery() over saved queries in the alert rule editor and when a
// panel switches datasource, so `editorMode` alone proves nothing.
describe('resolveMode', () => {
  test('a legacy query (no builder state) is Code', () => {
    expect(resolveMode({ refId: 'A', sql: 'SELECT 1' })).toBe('code');
  });

  test('defaults merged over a legacy alert query stay Code', () => {
    const merged = { ...builderDefaultQuery(), refId: 'A', sql: 'SELECT avg(v) FROM cpu WHERE $__timeFilter(time)' };
    expect(merged.editorMode).toBe('builder');
    expect(resolveMode(merged)).toBe('code');
  });

  test('a datasource switch from Postgres (rawSql, empty sql) is Code', () => {
    const switched = { ...builderDefaultQuery(), refId: 'A', sql: '', rawSql: 'SELECT 1' };
    expect(resolveMode(switched)).toBe('code');
  });

  test('a brand-new query is Builder', () => {
    expect(resolveMode({ ...builderDefaultQuery(), refId: 'A' })).toBe('builder');
  });

  test("a bare new query (Grafana 12.3 creates a new panel's query without defaults) is Builder", () => {
    expect(resolveMode({ refId: 'A' })).toBe('builder');
    expect(resolveMode({ refId: 'A', sql: '' })).toBe('builder');
    // ...but a saved query without builder state never is.
    expect(resolveMode({ refId: 'A', sql: 'SELECT 1' })).toBe('code');
    expect(resolveMode({ refId: 'A', rawSql: 'SELECT 1' })).toBe('code');
    expect(resolveMode({ refId: 'A', editorMode: 'code' })).toBe('code');
  });

  test('a committed builder query is Builder', () => {
    expect(resolveMode(builderQuery())).toBe('builder');
  });

  test('SQL edited outside the builder (dashboard JSON) is Code, with the SQL kept', () => {
    expect(resolveMode({ ...builderQuery(), sql: committed + ' LIMIT 5' })).toBe('code');
  });

  test('unknown builder or generator versions are Code', () => {
    const q = builderQuery();
    expect(resolveMode({ ...q, builder: { ...q.builder!, version: 2 as never } })).toBe('code');
    expect(resolveMode({ ...q, builder: { ...q.builder!, generatorVersion: 2 as never } })).toBe('code');
    expect(resolveMode({ ...q, builder: { ...q.builder!, generatorVersion: undefined as never } })).toBe('code');
  });

  test('Code chosen explicitly is Code even when the SQL still matches', () => {
    expect(resolveMode({ ...builderQuery(), editorMode: 'code' })).toBe('code');
  });
});

describe('codeModeChange', () => {
  test('merged-in builder defaults are dropped on the first Code edit, never saved', () => {
    const merged: ArcQuery = { ...(builderDefaultQuery() as ArcQuery), refId: 'A', sql: 'SELECT 1' };
    expect(isPristineDefault(merged)).toBe(true);
    const next = codeModeChange(merged, { sql: 'SELECT 2' });
    expect(next).toEqual({ refId: 'A', format: 'time_series', sql: 'SELECT 2' });
  });

  test('real builder state is kept, so the user can go back to it', () => {
    const q = { ...builderQuery(), editorMode: 'code' as const };
    const next = codeModeChange(q, { sql: 'SELECT 2' });
    expect(next.builder).toEqual(q.builder);
    expect(resolveMode(next)).toBe('code');
  });
});
