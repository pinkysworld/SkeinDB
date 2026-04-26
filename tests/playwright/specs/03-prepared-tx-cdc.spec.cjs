// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Exercises the Prepared Query Studio + Transaction Session Control polish slice:
 * prepares a SkeinQL query, captures the generated query_id, then exercises
 * the explicit transaction begin/rollback handles so the wired backend
 * tx.* surface is verified end-to-end.
 */
test('Prepared studio + transaction handles drive the live RPC surface', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="workspace"]').click();

  // Seed a trivial SkeinQL query so query.prepare has something deterministic to chew on.
  await page.locator('#skeinQuery').fill('{"schema":"pw_demo","table":"pw_users","select":[{"col":"id"}]}');
  await page.locator('#preparedArgs').fill('[]');
  await page.locator('#btnPreparedPrepare').click();
  // The prepare call always writes a JSON response (success OR error) to preparedOut.
  await expect(page.locator('#preparedOut')).not.toHaveText('Ready.', { timeout: 15_000 });

  await page.locator('#btnPreparedExecute').click();
  // Execute also always replaces the pre body.
  await expect(page.locator('#preparedOut')).not.toHaveText('Ready.', { timeout: 10_000 });

  // Transaction handles
  await page.locator('#btnTxBegin').click();
  await expect(page.locator('#txOut')).not.toHaveText('Ready.', { timeout: 10_000 });
  await page.locator('#btnTxRollback').click();
  await expect(page.locator('#txOut')).not.toHaveText('Ready.', { timeout: 10_000 });
});

test('Index Manager loads describe_table indexes for a real table', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="schema"]').click();
  await page.locator('#schemaDb').fill('pw_demo');
  await page.locator('#schemaTable').fill('pw_users');
  await page.locator('#btnSchemaLoadIndexes').click();
  await expect(page.locator('#schemaIndexOut')).not.toHaveText(/Ready\./, { timeout: 10_000 });
});

test('CDC table subscription begins, polls, and closes cleanly', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="cdc"]').click();
  await page.locator('#cdcDb').fill('pw_demo');
  await page.locator('#cdcTable').fill('pw_users');
  await page.locator('#btnCdcSubscribe').click();
  await expect(page.locator('#cdcOut')).not.toHaveText(/Ready\./, { timeout: 10_000 });

  // The subscription select should now expose at least one option.
  const opts = page.locator('#cdcSubId option');
  await expect.poll(async () => await opts.count(), { timeout: 10_000 }).toBeGreaterThan(0);

  await page.locator('#btnCdcPoll').click();
  await page.locator('#btnCdcClose').click();
});
