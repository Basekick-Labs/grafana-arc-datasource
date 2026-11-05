import { test, expect } from '@grafana/plugin-e2e';

// No Arc runs in CI: these check that the editor renders and switches modes,
// not metadata or results. The query builder's flows are covered by Jest
// component tests and the live matrix.

test('smoke: a new query opens in the visual builder', async ({ panelEditPage, readProvisionedDataSource }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  const row = panelEditPage.getQueryEditorRow('A');
  await expect(row.getByTestId('arc-query-builder')).toBeVisible();
  await expect(row.getByText('select measurement')).toBeVisible();
});

test('smoke: Code mode shows the SQL editor', async ({ panelEditPage, readProvisionedDataSource }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  const row = panelEditPage.getQueryEditorRow('A');
  await row.getByLabel('Code').click();
  await expect(row.getByPlaceholder('SELECT * FROM systems.cpu ORDER BY time DESC LIMIT 100')).toBeVisible();
});

test('builder and code round-trip, with a confirmation before leaving Code', async ({
  page,
  panelEditPage,
  readProvisionedDataSource,
}) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  const row = panelEditPage.getQueryEditorRow('A');
  await row.getByLabel('Code').click();
  await expect(row.getByTestId('arc-query-builder')).toHaveCount(0);
  await row.getByLabel('Builder').click();
  await page.getByRole('button', { name: 'Discard code and switch' }).click();
  await expect(row.getByTestId('arc-query-builder')).toBeVisible();
});
