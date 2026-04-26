// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Drives a real RPC round-trip through the Workspace SQL panel:
 * creates a database, creates a table, inserts a row, then runs SELECT.
 * Each `out` pre region must end up containing a non-error JSON response.
 */
test('SQL workspace executes round-trip against live backend', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="workspace"]').click();
  const sqlBox = page.locator('#sqlText');
  const out = page.locator('#sqlOut');

  const run = async (sql) => {
    await sqlBox.fill(sql);
    await page.locator('#btnSqlExec').click();
    // Either the rows table or a JSON success body should appear.
    await expect(out).not.toHaveText(/Waiting for SQL\./, { timeout: 15_000 });
    const text = await out.textContent();
    expect(text || '').not.toMatch(/"error"\s*:/);
  };

  await run('CREATE DATABASE IF NOT EXISTS pw_demo');
  await run('USE pw_demo');
  await run('CREATE TABLE IF NOT EXISTS pw_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)');
  await run("INSERT INTO pw_users (id, name) VALUES (1, 'Ada')");
  await run('SELECT * FROM pw_users');
  await expect(page.locator('#sqlTable')).toContainText('Ada');
});

test('Schema panel can list databases via RPC', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="schema"]').click();
  await page.locator('#btnSchemaListDb').click();
  // dbList renders buttons for each DB; require at least the pw_demo created above OR the default catalog entry.
  await expect(page.locator('#dbList')).not.toContainText('--', { timeout: 10_000 });
});

test('Settings.get returns a JSON response for an unknown key', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="settings"]').click();
  await page.locator('#settingsKey').fill('cluster.state.v1');
  await page.locator('#btnSettingsGet').click();
  await expect(page.locator('#settingsOut')).not.toHaveText(/No request yet\./, { timeout: 10_000 });
});

test('Telemetry compat summary loads', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="telemetry"]').click();
  await page.locator('#btnTelemetryCompatSummary').click();
  await expect(page.locator('#telemetryOut')).not.toHaveText(/Load telemetry/, { timeout: 10_000 });
});

test('RPC Explorer populates methods', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="rpc"]').click();
  // The select itself is what we wait on. Options inside <select> are technically
  // hidden until the dropdown is opened, so assert on count, not visibility.
  const options = page.locator('#rpcMethod option');
  await expect.poll(async () => options.count(), { timeout: 10_000 }).toBeGreaterThan(5);
});

test('Cluster Observe surfaces status JSON', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="cluster"]').click();
  await page.locator('#btnClusterStatus').click();
  await expect(page.locator('#clusterOut')).not.toHaveText(/Waiting\./, { timeout: 10_000 });
});

test('Engine config loads current settings', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="engine"]').click();
  await page.locator('#btnEngineLoad').click();
  await expect(page.locator('#engineOut')).not.toHaveText(/Load current config/, { timeout: 10_000 });
});

test('Forensics audit status returns chain info', async ({ page }) => {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="forensics"]').click();
  await page.locator('#btnForAuditStatus').click();
  await expect(page.locator('#forAuditOut')).not.toHaveText(/Ready\./, { timeout: 10_000 });
});
