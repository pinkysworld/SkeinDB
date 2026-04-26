# Playwright E2E for SkeinAdmin

End-to-end tests that drive the real `skeindb serve` backend through the SkeinAdmin UI.

## Run

```bash
# from this directory
cd tests/playwright
npm install
npx playwright test
```

The Playwright config runs `cargo run -p skeindb --bin skeindb -- serve` so the
embedded SkeinAdmin HTML/JS always comes from the current source tree. It uses a
per-run temporary data directory below `tests/playwright/.tmp-data/`, port 18080,
and disables the MySQL/PG listeners.

Useful overrides:

- `SKEINDB_BIN=/path/to/skeindb` runs an explicit binary instead of `cargo run`.
- `SKEINDB_HTTP_PORT=18081` changes the HTTP port.
- `SKEINDB_CLUSTER_PORT=19091` changes the cluster listener port.
- `SKEINDB_DATA_DIR=/tmp/skeindb-e2e` pins the data directory.
- `SKEINDB_REUSE_SERVER=1` reuses an already-running server at the configured URL.
