// @ts-check
const { test, expect } = require('@playwright/test');

const PANELS = [
  { panel: 'overview', hero: 'Operator Command Center' },
  { panel: 'workspace', hero: 'Workspace Mission Control' },
  { panel: 'schema', hero: 'Schema Command Deck' },
  { panel: 'data', hero: 'Data Workbench' },
  { panel: 'cluster', hero: 'Cluster Operations' },
  { panel: 'settings', hero: 'Settings & Feature Flags' },
  { panel: 'engine', hero: 'Engine Configuration' },
  { panel: 'users', hero: 'Users & Grants' },
  { panel: 'telemetry', hero: 'Telemetry & Workload Insights' },
  { panel: 'security', hero: 'Security Center' },
  { panel: 'encryption', hero: 'Encryption Control' },
  { panel: 'import', hero: 'Import / Export' },
  { panel: 'cdc', hero: 'CDC Control Surface' },
  { panel: 'replay', hero: 'Time Travel & Replay' },
  { panel: 'research', hero: 'Research Agenda Dashboard' },
  { panel: 'vectors', hero: 'Vector Search' },
  { panel: 'privacy', hero: 'Privacy Controls' },
  { panel: 'forensics', hero: 'Forensic Audit Chain' },
  { panel: 'views', hero: 'Incremental Views' },
  { panel: 'merge', hero: 'Merge & CRDT Policies' },
  { panel: 'wasm', hero: 'Wasm Query Operators' },
  { panel: 'advisor', hero: 'Index Advisor' },
  { panel: 'migration', hero: 'Migration & Compatibility' },
  { panel: 'nl', hero: 'Natural Language Lab' },
  { panel: 'rpc', hero: 'RPC Explorer' },
];

test.describe('SkeinAdmin layout & navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/admin');
    await expect(page.locator('.app-shell')).toBeVisible();
  });

  test('all panels render their section-band hero', async ({ page }) => {
    for (const { panel, hero } of PANELS) {
      const navItem = page.locator(`.nav-item[data-panel="${panel}"]`).first();
      await navItem.scrollIntoViewIfNeeded();
      await navItem.click();
      const section = page.locator(`section.panel[data-panel="${panel}"].active`);
      await expect(section).toBeVisible();
      // Heading must be present somewhere in the section markup, even when
      // dynamic dashboard JS rewrites parts of the panel below the fold.
      const heading = section.locator(`h2:has-text("${hero}")`).first();
      await expect(heading).toBeAttached({ timeout: 5_000 });
    }
  });

  test('command palette opens with Cmd+K and filters', async ({ page }) => {
    // The handler matches `e.key === 'k'`, so dispatch the lowercase key directly
    // to avoid platform-specific Shift behavior in Playwright's Meta+K mapping.
    await page.evaluate(() => {
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', metaKey: true, bubbles: true, cancelable: true }));
    });
    const overlay = page.locator('#cmdPalette');
    await expect(overlay).toHaveClass(/open/, { timeout: 5_000 });
    await page.locator('#cmdInput').fill('schema');
    await expect(page.locator('#cmdResults .cmd-palette-item').first()).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(overlay).not.toHaveClass(/open/);
  });

  test('theme toggle flips dark mode', async ({ page }) => {
    const html = page.locator('html');
    const before = await html.getAttribute('class');
    await page.locator('.theme-toggle').click();
    const after = await html.getAttribute('class');
    expect(after).not.toBe(before);
  });

  test('overview command-center quick actions are wired', async ({ page }) => {
    await page.locator('.nav-item[data-panel="overview"]').click();
    for (const id of [
      'btnQuickCreateDb',
      'btnQuickCreateTable',
      'btnQuickInsertRow',
      'btnQuickBrowseData',
      'btnQuickCluster',
    ]) {
      await expect(page.locator(`#${id}`)).toBeVisible();
    }
  });
});
