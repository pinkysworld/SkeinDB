// @ts-check
const { defineConfig } = require('@playwright/test');
const path = require('path');

const PORT = process.env.SKEINDB_HTTP_PORT || '18080';
const CLUSTER_PORT = process.env.SKEINDB_CLUSTER_PORT || '19090';
const BASE_URL = `http://127.0.0.1:${PORT}`;
const REPO_ROOT = path.resolve(__dirname, '../..');
const DATA_DIR = process.env.SKEINDB_DATA_DIR || path.resolve(__dirname, '.tmp-data', `${process.pid}`);
const SKEINDB_BIN = process.env.SKEINDB_BIN;

const serverCommand = SKEINDB_BIN
  ? `"${SKEINDB_BIN}" serve`
  : `cargo run --manifest-path "${path.join(REPO_ROOT, 'Cargo.toml')}" -p skeindb --bin skeindb -- serve`;

module.exports = defineConfig({
  testDir: './specs',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: BASE_URL,
    headless: true,
    viewport: { width: 1440, height: 900 },
    trace: 'retain-on-failure',
  },
  webServer: {
    command: `${serverCommand} --data "${DATA_DIR}" --bind 127.0.0.1 --http ${PORT} --mysql 0 --pg 0 --cluster-port ${CLUSTER_PORT}`,
    url: `${BASE_URL}/admin`,
    reuseExistingServer: process.env.SKEINDB_REUSE_SERVER === '1',
    timeout: 90_000,
    stdout: 'ignore',
    stderr: 'pipe',
  },
});
