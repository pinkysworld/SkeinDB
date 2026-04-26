// @ts-check
const { defineConfig } = require('@playwright/test');
const path = require('path');

const PORT = process.env.SKEINDB_HTTP_PORT || '18080';
const BASE_URL = `http://127.0.0.1:${PORT}`;

module.exports = defineConfig({
  testDir: './specs',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: BASE_URL,
    headless: true,
    viewport: { width: 1440, height: 900 },
    trace: 'retain-on-failure',
  },
  webServer: {
    command: `"${path.resolve(__dirname, '../../target/debug/skeindb')}" serve --data "${path.resolve(__dirname, '.tmp-data')}" --bind 127.0.0.1 --http ${PORT} --mysql 0 --pg 0 --cluster-port 19090`,
    url: `${BASE_URL}/admin`,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
    stdout: 'ignore',
    stderr: 'pipe',
  },
});
