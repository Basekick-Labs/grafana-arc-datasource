// @grafana/runtime pulls in the whole @grafana/ui bundle, which does not load
// under jsdom. The datasource only needs DataSourceWithBackend (for the class
// to extend) and getTemplateSrv (unused by the pure function under test), so
// both are stubbed here.
jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    constructor(public instanceSettings: unknown) {}
  },
  getTemplateSrv: () => ({ replace: (s: string) => s }),
  frameToMetricFindValue: () => [],
}));

import { ArcDataSource } from './datasource';
import { DataSourceInstanceSettings } from '@grafana/data';
import { ArcDataSourceOptions } from './types';

/**
 * Tests `interpolateVariable` as a pure function with explicit variable
 * fixtures. It is deliberately NOT driven through a mocked `getTemplateSrv`:
 * upstream Grafana never calls this hook for `${var:raw}`, `:csv`, `:regex` or
 * `:singlequote`, nor for a custom `allValue`, and it collapses a one-element
 * array to a string before calling it. Asserting those behaviours against a
 * mock would test the mock, not Grafana. The dispatch semantics belong in a
 * provisioned fixtures dashboard.
 *
 * The rule under test is Grafana's own, from
 * `packages/grafana-sql/src/datasource/SqlDatasource.ts`: quote only when
 * `variable.multi || variable.includeAll`.
 */
function makeDatasource(): ArcDataSource {
  const settings = {
    id: 1,
    uid: 'test',
    type: 'basekick-arc-datasource',
    name: 'Arc',
    jsonData: {},
    meta: {},
    readOnly: false,
    access: 'proxy',
  } as unknown as DataSourceInstanceSettings<ArcDataSourceOptions>;
  return new ArcDataSource(settings);
}

// Minimal shapes standing in for Grafana's VariableWithMultiSupport. Only the
// two fields the rule consults are relevant.
const single = { name: 'server', multi: false, includeAll: false } as any;
const multi = { name: 'cpu', multi: true, includeAll: false } as any;
const withAll = { name: 'disk', multi: false, includeAll: true } as any;

describe('interpolateVariable', () => {
  const ds = makeDatasource();

  describe('single-value variables (the author supplies the quotes)', () => {
    it('does not add surrounding quotes', () => {
      // WHERE host = '$server' -> WHERE host = 'h01'
      expect(ds.interpolateVariable('h01', single)).toBe('h01');
    });

    it('keeps a regex-anchored value usable: host ~ \'^$server$\'', () => {
      // The 1.3.2 rule produced '^'h01'$' here. 13 production panels use this.
      expect(ds.interpolateVariable('h01', single)).toBe('h01');
    });

    it('still doubles embedded quotes, so a payload cannot escape the literal', () => {
      expect(ds.interpolateVariable("x' OR 1=1--", single)).toBe("x'' OR 1=1--");
    });
  });

  describe('multi-value and All variables (the author cannot pre-quote)', () => {
    it('quotes a multi-value variable', () => {
      // WHERE cpu = $cpu -> WHERE cpu = 'cpu38'
      expect(ds.interpolateVariable('cpu38', multi)).toBe("'cpu38'");
    });

    it('quotes an includeAll variable', () => {
      expect(ds.interpolateVariable('/dev/sda', withAll)).toBe("'/dev/sda'");
    });

    it('quotes each element of an array and joins with commas', () => {
      // WHERE host IN ($hosts)
      expect(ds.interpolateVariable(['a', 'b'], multi)).toBe("'a','b'");
    });

    it('escapes embedded quotes when quoting', () => {
      expect(ds.interpolateVariable("O'Brien", multi)).toBe("'O''Brien'");
    });
  });

  describe('non-string values', () => {
    it('passes numbers through unquoted', () => {
      expect(ds.interpolateVariable(42, single)).toBe(42);
    });
  });

  describe('built-in macros', () => {
    // Grafana routes its own $__interval, $__from, $__to through this same
    // hook. They are non-multi, so the rule leaves them bare and
    // `INTERVAL '$__interval'` becomes INTERVAL '10 seconds' rather than the
    // 1.3.2 output INTERVAL ''10 seconds''.
    it('leaves an interval value bare so it works inside the author quotes', () => {
            const builtin = { name: '__interval', multi: false, includeAll: false } as any;
      expect(ds.interpolateVariable('10 seconds', builtin)).toBe('10 seconds');
    });
  });

  describe('missing variable metadata', () => {
    it('treats an undefined variable as single-value', () => {
            expect(ds.interpolateVariable('h01', undefined as any)).toBe('h01');
    });
  });
});
