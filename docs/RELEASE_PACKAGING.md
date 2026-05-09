# Release Packaging Notes

Last updated: 2026-05-09

SkeinDB release tags run `.github/workflows/release-packages.yml`, which builds the Linux binary tarball, source tarball, Debian package, checksums, and a rendered Homebrew formula.

The signed apt repository path is optional. To publish the `apt` branch, configure these repository secrets before pushing a release tag:

- `APT_GPG_PRIVATE_KEY`
- `APT_GPG_KEY_ID`
- `APT_GPG_PASSPHRASE` (only when the imported key requires one)

When those secrets are absent, `scripts/release/build_apt_repo.sh` still creates an unsigned repository layout for inspection, and the publish step skips the `apt` branch because signed `InRelease` and `pubkey.gpg` artifacts are not present.

The checked-in Homebrew formula may point at the previous release until the tag workflow finishes. The workflow renders the formula from the tagged source tarball SHA and commits the updated formula back to the default branch.
