# Release Packaging Notes

Last updated: 2026-05-09

SkeinDB release tags run `.github/workflows/release-packages.yml`, which builds the Linux binary tarball, source tarball, Debian package, checksums, a signed macOS binary tarball, and a rendered Homebrew formula.

## macOS signing

The release workflow includes a `macos-latest` job that builds `target/release/skeindb`, signs it with `codesign`, verifies the signature, and uploads:

- `skeindb-<version>-macos-<arch>.tar.gz`
- `skeindb-<version>-macos-<arch>.tar.gz.sha256`
- `skeindb-<version>-macos-<arch>-codesign.txt`

By default, CI uses an ad-hoc signature (`codesign --sign -`) so every macOS artifact is sealed and verification-friendly without requiring Apple account secrets. For a Developer ID signature on a Mac or a self-hosted runner with the certificate already installed, set `MACOS_CODESIGN_IDENTITY` or pass `--identity` to `scripts/release/build_macos_signed_artifact.sh`.

Local Developer ID or Apple Development signing example:

```bash
cargo build --release -p skeindb
MACOS_CODESIGN_IDENTITY="Developer ID Application: Example Inc (TEAMID)" \
	scripts/release/build_macos_signed_artifact.sh \
		--version 0.3.7 \
		--binary target/release/skeindb \
		--output dist
```

Use the exact signing identity from `security find-identity -v -p codesigning`. The script verifies with `codesign --verify --strict --verbose=2` before producing the archive.

## apt signing

The signed apt repository path is optional. To publish the `apt` branch, configure these repository secrets before pushing a release tag:

- `APT_GPG_PRIVATE_KEY`
- `APT_GPG_KEY_ID`
- `APT_GPG_PASSPHRASE` (only when the imported key requires one)

When those secrets are absent, `scripts/release/build_apt_repo.sh` still creates an unsigned repository layout for inspection, and the publish step skips the `apt` branch because signed `InRelease` and `pubkey.gpg` artifacts are not present.

The checked-in Homebrew formula may point at the previous release until the tag workflow finishes. The workflow renders the formula from the tagged source tarball SHA and commits the updated formula back to the default branch.
