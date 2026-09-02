import { test, expect } from '@grafana/plugin-e2e';

test('smoke: should render query editor', async ({ panelEditPage, readProvisionedDataSource }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  await expect(
    panelEditPage.getQueryEditorRow('A').getByPlaceholder('SELECT * FROM systems.cpu ORDER BY time DESC LIMIT 100')
  ).toBeVisible();
});
