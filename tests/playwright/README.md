# Playwright E2E for SkeinAdmin

End-to-end tests that drive the real `skeindb serve` backend through the SkeinAdmin UI.

## Run

```bash
# from the repo root, ensure debug binary exists
cargo build -p skeindb

# from this directory
cd tests/playwright
npm install
npx playwright test
```

The Playwright config boots `target/debug/skeindb serve` against a temporary
data directory at `tests/playwright/.tmp-data/`, port 18080, with MySQL/PG
listeners disabled. Set `CI=1` to force a fresh server boot per run.
