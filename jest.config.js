// force timezone to UTC to allow tests to work regardless of local timezone
// generally used by snapshots, but can affect specific tests
process.env.TZ = 'UTC';

const { grafanaESModules, nodeModulesToTransform } = require('./.config/jest/utils');

module.exports = {
  // Jest configuration provided by Grafana scaffolding
  ...require('./.config/jest.config'),
  // The query builder's component tests render @grafana/ui, which pulls in
  // these ESM-only packages beyond the scaffold's list.
  transformIgnorePatterns: [nodeModulesToTransform([...grafanaESModules, '@react-hookz', '@ver0'])],
};
