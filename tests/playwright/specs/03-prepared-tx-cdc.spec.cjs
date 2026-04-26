// @ts-check
const { test, expect } = require('@playwright/test');
const { seedWorkspaceTable, waitForRpc } = require('./helpers.cjs');

const preparedQueryJson = JSON.stringify({
  body: {
    select: {
      projection: [{ expr: { col: 'id' } }],
      from: [{ db: 'pw_demo', table: 'pw_users' }],
    },
  },
});

/**
 * Exercises the Prepared Query Studio + Transaction Session Control polish slice:
 * prepares a SkeinQL query, captures the generated query_id, then exercises
 * the explicit transaction begin/rollback handles so the wired backend
 * tx.* surface is verified end-to-end.
 */
test('Prepared studio + transaction handles drive the live RPC surface', async ({ page }) => {
  await seedWorkspaceTable(page);

  // Seed a trivial SkeinQL query so query.prepare has something deterministic to chew on.
  await page.locator('#skeinQuery').fill(preparedQueryJson);
  await page.locator('#preparedArgs').fill('[]');
  await waitForRpc(page, 'query.prepare', () => page.locator('#btnPreparedPrepare').click());
  await expect.poll(async () => (await page.locator('#preparedQueryId').inputValue()).length, {
    timeout: 10_000,
  }).toBeGreaterThan(0);

  await waitForRpc(page, 'query.execute_prepared', () => page.locator('#btnPreparedExecute').click());

  // Transaction handles
  await waitForRpc(page, 'tx.begin', () => page.locator('#btnTxBegin').click());
  await expect.poll(async () => (await page.locator('#txId').inputValue()).startsWith('tx_'), {
    timeout: 10_000,
  }).toBeTruthy();
  await waitForRpc(page, 'tx.rollback', () => page.locator('#btnTxRollback').click());
});

test('Index Manager loads describe_table indexes for a real table', async ({ page }) => {
  await seedWorkspaceTable(page);
  await page.locator('.nav-item[data-panel="schema"]').click();
  await page.locator('#schemaDb').fill('pw_demo');
  await page.locator('#schemaTable').fill('pw_users');
  await waitForRpc(page, 'schema.describe_table', () => page.locator('#btnSchemaLoadIndexes').click());
  await expect(page.locator('#schemaIndexOut')).not.toHaveText(/Ready\./, { timeout: 10_000 });
});

test('CDC table subscription begins, polls, and closes cleanly', async ({ page }) => {
  await seedWorkspaceTable(page);
  await page.locator('.nav-item[data-panel="cdc"]').click();
  await page.locator('#cdcDb').fill('pw_demo');
  await page.locator('#cdcTable').fill('pw_users');
  await waitForRpc(page, 'cdc.subscribe_table', () => page.locator('#btnCdcSubscribe').click());
  await expect(page.locator('#cdcOut')).not.toHaveText(/Ready\./, { timeout: 10_000 });

  // The subscription select should now expose at least one option.
  const opts = page.locator('#cdcSubId option');
  await expect.poll(async () => await opts.count(), { timeout: 10_000 }).toBeGreaterThan(0);

  await waitForRpc(page, 'cdc.poll', () => page.locator('#btnCdcPoll').click());
  await page.locator('#btnCdcClose').click();
  await expect(page.locator('#modalOverlay.active')).toBeVisible();
  const confirmClose = page.locator('#modalActions button').filter({ hasText: 'Close' });
  await expect(confirmClose).toBeVisible();
  await waitForRpc(page, 'cdc.close', () => confirmClose.evaluate((button) => button.click()));
});
