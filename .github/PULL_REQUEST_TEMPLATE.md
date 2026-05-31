<!--
Thanks for contributing to SkeinDB! Please keep PRs small (<~800 lines net change
unless explicitly requested) and follow the non-negotiables in AGENTS.md.
-->

## Summary

<!-- What does this change do and why? -->

## Changes

<!-- Bullet the key changes. -->
-

## Testing

<!-- How did you verify this? Paste relevant command output. -->
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo test --all --all-features`
- [ ] SkeinAdmin Playwright suite (if UI changed)

## Checklist

- [ ] Tests added/updated for the change
- [ ] Docs updated if behavior/format changed
- [ ] No on-disk format change without a record-version bump + `docs/ON_DISK_FORMAT.md` update
- [ ] No silent breaking changes to the MySQL/PostgreSQL wire protocol
