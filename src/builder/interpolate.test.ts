import { CustomVariable, EmbeddedScene, SceneVariableSet, sceneGraph } from '@grafana/scenes';
import { builderFormat, rawSingle, VariableUseError } from './interpolate';

// Interpolation through Grafana's own engine (@grafana/scenes, what
// dashboards use), not a mock: the plan v3 review (B2) found the format that
// matters is decided there, and that `:sqlstring` gets several cases wrong.
function interpolate(text: string, value: string | string[], opts: { multi?: boolean; all?: boolean } = {}) {
  const v = new CustomVariable({
    name: 'host',
    query: 'a,b,o\'neil,say "hi"',
    isMulti: opts.multi ?? Array.isArray(value),
    includeAll: opts.all ?? false,
    value,
    text: value,
    // Loaded options: what "All" expands to on a dashboard.
    options: ['a', 'b', "o'neil", 'say "hi"'].map((o) => ({ label: o, value: o })),
  });
  const scene = new EmbeddedScene({ $variables: new SceneVariableSet({ variables: [v] }), body: undefined as never });
  return sceneGraph.interpolate(scene, text, undefined, builderFormat as never);
}

describe('builderFormat through scenes', () => {
  test('single value: one quoted literal', () => {
    expect(interpolate('"host" IN ($host)', 'a', { multi: false })).toBe(`"host" IN ('a')`);
  });

  test('multi value: each quoted, comma-joined', () => {
    expect(interpolate('"host" IN ($host)', ['a', 'b'])).toBe(`"host" IN ('a','b')`);
  });

  test('a value with \' is doubled; a value with " is left alone (no backslash escape)', () => {
    expect(interpolate('"host" IN ($host)', ["o'neil", 'say "hi"'])).toBe(`"host" IN ('o''neil','say "hi"')`);
  });

  test('All (no custom All value) expands to every option', () => {
    expect(interpolate('"host" IN ($host)', ['$__all'], { multi: true, all: true })).toBe(
      `"host" IN ('a','b','o''neil','say "hi"')`
    );
  });

  test('an empty multi-selection is NULL, not IN ()', () => {
    expect(interpolate('"host" IN ($host)', [])).toBe('"host" IN (NULL)');
    expect(interpolate('coalesce("host" NOT IN ($host), true)', [])).toBe('coalesce("host" NOT IN (NULL), true)');
  });

  test('${host} is the same reference', () => {
    expect(interpolate('"host" IN (${host})', ['a'])).toBe(`"host" IN ('a')`);
  });

  test('an explicit :raw (measurement position) bypasses the quoting', () => {
    expect(interpolate('FROM "${host:raw}"', 'a', { multi: false })).toBe('FROM "a"');
  });
});

describe('rawSingle', () => {
  test('one value, as is', () => {
    expect(rawSingle('cpu')).toBe('cpu');
    expect(rawSingle(['cpu'])).toBe('cpu');
    expect(rawSingle(7)).toBe('7');
  });

  test('several values cannot be one name', () => {
    expect(() => rawSingle(['a', 'b'])).toThrow(VariableUseError);
    expect(() => rawSingle([])).toThrow(VariableUseError);
  });
});

test('the transformation macros survive interpolation; $__interval inside them is quoted', () => {
  const v = new CustomVariable({ name: 'host', query: 'a', value: 'a', text: 'a' });
  const scene = new EmbeddedScene({ $variables: new SceneVariableSet({ variables: [v] }), body: undefined as never });
  const out = sceneGraph.interpolate(
    scene,
    'WHERE $__timeFilterLookback(time, $__interval, 1) AND "host" IN ($host) QUALIFY "__t" >= $__timeGroupStart($__interval)',
    { __interval: { text: '10s', value: '10s' } },
    builderFormat as never
  );
  expect(out).toBe(
    `WHERE $__timeFilterLookback(time, '10s', 1) AND "host" IN ('a') QUALIFY "__t" >= $__timeGroupStart('10s')`
  );
});
