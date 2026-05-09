# Changelog

## v0.3.8 - 2026-05-09

- Adds a comprehensive **Help & Docs** panel to SkeinAdmin: quick-start checklist, panel reference table with one-click jumps, R01-R20 research-track index with hardness pills and primary RPC methods, keyboard-shortcut and deep-link reference, glossary, and links to the canonical documentation site.
- Wires Help into the left nav, top tabs, the topbar `? Help` button, and a `?` keyboard shortcut.
- Adds live filter search across panel and research entries inside the Help Center.
- Locks the new Help panel surface with `skeinadmin_help_panel_exposes_comprehensive_documentation_center` so docs claims stay test-backed.
- Keeps the research backlog status honest: 29 done / 80 open, 18 hardened / 2 prototype tracks (R18 perf replay and R19 Wasm operators remain at prototype level).
- Updates README, SKEINADMIN.md, the public website, and the docs site to advertise v0.3.8 and the new Help Center.
- Refreshes the Homebrew formula to v0.3.8.

## v0.3.7 - 2026-05-09

- Promotes the post-R12/R17/R20 hardening line into a versioned release-prep state.
- Keeps the research backlog status honest: 29 done / 80 open, with R18 and R19 still prototype-level.
- Adds R19 Wasm plan artifact metadata, inspect RPC, host-backed edge package helper, and current SkeinAdmin Wasm controls.
- Adds R18 performance-annotated replay bundles with storage/cache/timing metadata and replay variance reports.
- Adds replay bundle primary-key redaction for `maintenance.replay.export`, SkeinAdmin replay export, and `skeindb replay export`.
- Updates public website, docs, and status copy to the current 18 hardened / 2 prototype research-track state.
- Adds review-driven regression coverage for NL approval-token mismatch behavior and migration-intent combined/false-positive cases.
- Carries the Wasmtime 43.0.2 security update and R17 migration report exporter from the prior batch.
