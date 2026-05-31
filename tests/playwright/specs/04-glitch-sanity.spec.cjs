// @ts-check
// Live "glitch sanity" sweep: walks every nav panel and the command palette
// while capturing uncaught exceptions and console errors. This catches runtime
// glitches (bad selectors, undefined references, render crashes) that the
// happy-path RPC specs do not exercise.
const { test, expect } = require('@playwright/test');

/** Console messages that are benign in this environment (network noise, favicon). */
function isIgnorableConsoleError(text) {
  return (
    /favicon\.ico/i.test(text) ||
    /Failed to load resource/i.test(text) ||
    /\/api\/v1\/rpc/i.test(text) // backend RPC error payloads are surfaced in-panel, not a UI glitch
  );
}

test('no uncaught exceptions or console errors while walking every panel', async ({ page }) => {
  /** @type {string[]} */
  const pageErrors = [];
  /** @type {string[]} */
  const consoleErrors = [];

  page.on('pageerror', (err) => {
    pageErrors.push(`${err.name}: ${err.message}`);
  });
  page.on('console', (msg) => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (!isIgnorableConsoleError(text)) consoleErrors.push(text);
  });

  await page.goto('/admin');
  await expect(page.locator('.app-shell')).toBeVisible();

  const navItems = page.locator('.nav-item[data-panel]');
  const count = await navItems.count();
  expect(count, 'expected the sidebar to expose nav items').toBeGreaterThan(20);

  for (let i = 0; i < count; i++) {
    const item = navItems.nth(i);
    const panel = await item.getAttribute('data-panel');
    await item.scrollIntoViewIfNeeded();
    await item.click();
    // The matching section must become active and visible.
    const section = page.locator(`section.panel[data-panel="${panel}"]`);
    await expect(section, `panel "${panel}" should activate`).toHaveClass(/active/, { timeout: 5_000 });
    // Allow any deferred dashboard JS to run.
    await page.waitForTimeout(120);
  }

  // Open and close the command palette (exercises the global keydown + filter path).
  await page.evaluate(() => {
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', metaKey: true, bubbles: true, cancelable: true }));
  });
  await expect(page.locator('#cmdPalette')).toHaveClass(/open/, { timeout: 5_000 });
  await page.locator('#cmdInput').fill('schema');
  await page.keyboard.press('Escape');

  expect(pageErrors, `uncaught JS exceptions:\n${pageErrors.join('\n')}`).toHaveLength(0);
  expect(consoleErrors, `console errors:\n${consoleErrors.join('\n')}`).toHaveLength(0);
});

test('Easy Viewer and Help panels render without crashing', async ({ page }) => {
  /** @type {string[]} */
  const pageErrors = [];
  page.on('pageerror', (err) => pageErrors.push(`${err.name}: ${err.message}`));

  await page.goto('/admin');

  await page.locator('.nav-item[data-panel="easy"]').click();
  const easy = page.locator('section.panel[data-panel="easy"]');
  await expect(easy).toHaveClass(/active/);
  // Easy Viewer uses a custom breadcrumb/tree layout (no <h2>).
  await expect(easy.locator('.easy-layout')).toBeVisible();
  await expect(easy.locator('#easyTree')).toBeAttached();

  await page.locator('.nav-item[data-panel="help"]').click();
  const help = page.locator('section.panel[data-panel="help"]');
  await expect(help).toHaveClass(/active/);
  await expect(help.locator('h2:has-text("SkeinAdmin Help Center")').first()).toBeAttached();

  expect(pageErrors, `uncaught JS exceptions:\n${pageErrors.join('\n')}`).toHaveLength(0);
});

test('unknown hash falls back to a valid panel instead of a blank screen', async ({ page }) => {
  /** @type {string[]} */
  const pageErrors = [];
  page.on('pageerror', (err) => pageErrors.push(`${err.name}: ${err.message}`));

  await page.goto('/admin#totally-unknown-panel');
  await expect(page.locator('.app-shell')).toBeVisible();

  // Exactly one panel must end up active so the operator never sees a blank screen.
  await expect.poll(async () => await page.locator('section.panel.active').count(), {
    timeout: 5_000,
  }).toBe(1);
  await expect(page.locator('section.panel[data-panel="overview"]')).toHaveClass(/active/);

  expect(pageErrors, `uncaught JS exceptions:\n${pageErrors.join('\n')}`).toHaveLength(0);
});
