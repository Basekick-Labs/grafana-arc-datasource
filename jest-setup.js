// Jest setup provided by Grafana scaffolding
import './.config/jest-setup';

// jsdom has no IntersectionObserver; @grafana/ui's dropdown menus (used by the
// query builder's segments) create one for their scroll indicators. Nothing
// under test depends on it reporting intersections.
if (typeof global.IntersectionObserver === 'undefined') {
  global.IntersectionObserver = class IntersectionObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
    takeRecords() {
      return [];
    }
  };
}

// jsdom has no canvas. @grafana/ui's SegmentInput sizes itself with
// measureText; an approximate width is all the tests need.
if (typeof HTMLCanvasElement !== 'undefined') {
  HTMLCanvasElement.prototype.getContext = function getContext() {
    return {
      font: '',
      measureText: (text) => ({ width: String(text).length * 7 }),
    };
  };
}
