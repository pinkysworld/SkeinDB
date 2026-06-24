# Contributing to SkeinDB

Thanks for your interest in improving SkeinDB! This guide summarizes the
expectations from `AGENTS.md` so contributions land smoothly.

## Non-negotiables

- **Stable Rust only.** No `unsafe` unless absolutely necessary; justify it in a
  comment if used.
- **Every change ships with tests.** Prefer unit tests plus at least one
  integration test when touching SQL or the wire protocols.
- **On-disk formats are versioned.** Do not change a record format without bumping
  its version and updating `docs/ON_DISK_FORMAT.md`.
- **Keep PRs small** — aim for <~800 lines of net change unless a larger change is
  explicitly requested.
- **No silent breaking changes** to the MySQL/PostgreSQL wire protocol.

## Local checks

Run the same checks CI runs before opening a PR:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
```

If you changed SkeinAdmin (`web/skeinadmin/`), run the end-to-end suite too:

```bash
cargo build -p skeindb --bin skeindb
cd tests/playwright
npm ci
npx playwright install --with-deps chromium
SKEINDB_BIN="$PWD/../../target/debug/skeindb" npx playwright test
```

If you changed docs, rebuild the static site to confirm it still generates:

```bash
python -m pip install markdown Pygments
python scripts/build_docs_site.py
```

## Working style

Favor depth over motion:

- **Drive one foundational outcome to done** rather than many trivial slices. "Small
  PR" means well-scoped, not artificially tiny — a coherent change that moves a real
  capability beats several sub-200-line slices that mostly touch docs.
- **Never add code, tests, or helpers solely to cite as evidence of progress.** If a
  primitive is not wired into a live path, it is not done — don't claim it.
- **Finish the primary write/read paths** (storage durability, crash recovery,
  streaming reads) before adding breadth that sits on an unfinished foundation.

## Documentation honesty

`docs/TRUE_STATUS_MATRIX.md` tracks what is actually hardened versus prototype.
Only claim what is implemented and verified — do not overstate status. The matrix is
a **periodic truth snapshot, not a per-commit changelog** — update it when reality
changes, and keep the running history in git and `CHANGELOG.md`.

## Commit and PR conventions

- Write focused commits with clear messages.
- Fill out the pull request template, including the testing checklist.
- Update relevant docs in the same PR when behavior or formats change.

## Licensing

SkeinDB is open-source with a commercial track (see `COMMERCIAL.md`). By
contributing, you agree your contributions are licensed under the repository's
license (`LICENSE`).
