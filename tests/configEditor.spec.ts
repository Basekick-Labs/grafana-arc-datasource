import { test, expect } from '@grafana/plugin-e2e';

test('smoke: should render config editor', async ({ createDataSourceConfigPage, readProvisionedDataSource, page }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await createDataSourceConfigPage({ type: ds.type });
  await expect(page.getByPlaceholder('http://localhost:8000')).toBeVisible();
  await expect(page.getByPlaceholder('Your Arc API key')).toBeVisible();
});

test('"Save & test" should fail when the API key is missing', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  page,
}) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  const configPage = await createDataSourceConfigPage({ type: ds.type });
  await page.getByPlaceholder('http://localhost:8000').fill('http://localhost:8000');
  // No API key entered: the backend rejects the instance at factory time.
  await expect(configPage.saveAndTest()).not.toBeOK();
});
