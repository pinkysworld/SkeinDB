// @ts-check
const { expect } = require('@playwright/test');

async function waitForRpc(page, method, action, timeout = 15_000) {
  const [response] = await Promise.all([
    page.waitForResponse((candidate) => {
      if (!candidate.url().endsWith('/api/v1/rpc')) return false;
      if (candidate.request().method() !== 'POST') return false;
      try {
        const body = JSON.parse(candidate.request().postData() || '{}');
        return body.method === method;
      } catch (_) {
        return false;
      }
    }, { timeout }),
    action(),
  ]);

  const body = await response.json();
  expect(response.ok(), `${method} HTTP status ${response.status()}: ${JSON.stringify(body)}`).toBeTruthy();
  expect(body.ok, `${method} RPC response: ${JSON.stringify(body, null, 2)}`).toBeTruthy();
  expect(body.error, `${method} should not return an RPC error`).toBeFalsy();
  return body.result;
}

async function runSql(page, sql) {
  await page.locator('#sqlText').fill(sql);
  return waitForRpc(page, 'sql.exec', () => page.locator('#btnSqlExec').click());
}

async function seedWorkspaceTable(page) {
  await page.goto('/admin');
  await page.locator('.nav-item[data-panel="workspace"]').click();
  await runSql(page, 'CREATE DATABASE IF NOT EXISTS pw_demo');
  await runSql(page, 'USE pw_demo');
  await runSql(page, 'CREATE TABLE IF NOT EXISTS pw_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)');
}

module.exports = { runSql, seedWorkspaceTable, waitForRpc };
