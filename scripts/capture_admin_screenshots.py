#!/usr/bin/env python3
"""Capture SkeinAdmin panel screenshots for the docs site.

Prereqs (already installed in this workspace):
  pip install playwright && python -m playwright install chromium

The server must be running on http://127.0.0.1:18080 with some seeded data
(databases 'shop' and 'demo' with a few tables/rows).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:18080/admin/"
OUT = Path(__file__).resolve().parent.parent / "docs" / "site" / "guide" / "assets" / "screenshots"
OUT.mkdir(parents=True, exist_ok=True)

# (filename, panel data-panel value)
PANELS = [
    ("admin-overview.png",       "overview"),
    ("admin-dashboard.png",      "telemetry"),
    ("admin-schema.png",         "schema"),
    ("admin-data-editor.png",    "data"),
    ("admin-sql.png",            "workspace"),
    ("admin-index-advisor.png",  "advisor"),
    ("admin-cluster.png",        "cluster"),
    ("admin-audit.png",          "forensics"),
]


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        page = ctx.new_page()

        page.goto(BASE, wait_until="networkidle")
        # Let the SPA fetch catalog etc.
        page.wait_for_selector('button.nav-item[data-panel="overview"]', timeout=10_000)
        time.sleep(1.0)

        for filename, panel in PANELS:
            print(f"[capture] {panel} -> {filename}", flush=True)
            sel = f'button.nav-item[data-panel="{panel}"]'
            try:
                page.click(sel, timeout=5_000)
            except Exception as e:
                print(f"  ! click failed ({e}); trying tab-btn", flush=True)
                try:
                    page.click(f'button.tab-btn[data-panel="{panel}"]', timeout=5_000)
                except Exception as e2:
                    print(f"  !! skipped panel {panel}: {e2}", flush=True)
                    continue
            # Wait for panel switch + any async data loads.
            page.wait_for_selector(f'section.panel[data-panel="{panel}"]', timeout=5_000)
            time.sleep(1.2)
            page.screenshot(path=str(OUT / filename), full_page=False)

        browser.close()
    print(f"Done. Wrote {len(PANELS)} screenshots to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
