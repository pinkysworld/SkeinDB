#!/usr/bin/env bash

set -euo pipefail

version=""
binary="target/release/skeindb"
output_dir="dist"
identity="${MACOS_CODESIGN_IDENTITY:--}"
artifact_arch="${MACOS_ARTIFACT_ARCH:-$(uname -m)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      version="$2"
      shift 2
      ;;
    --binary)
      binary="$2"
      shift 2
      ;;
    --output)
      output_dir="$2"
      shift 2
      ;;
    --identity)
      identity="$2"
      shift 2
      ;;
    --arch)
      artifact_arch="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$version" ]]; then
  echo "usage: build_macos_signed_artifact.sh --version 0.3.7 [--binary target/release/skeindb] [--output dist]" >&2
  exit 1
fi

if [[ ! -e "$binary" ]]; then
  echo "macOS binary or app bundle not found: $binary" >&2
  exit 1
fi

if ! command -v codesign >/dev/null 2>&1; then
  echo "codesign is required to build macOS release artifacts" >&2
  exit 1
fi

mkdir -p "$output_dir"

sign_args=(--force --sign "$identity")
if [[ "$identity" != "-" ]]; then
  sign_args+=(--options runtime --timestamp)
fi
if [[ -d "$binary" && "$binary" == *.app ]]; then
  sign_args+=(--deep)
fi

codesign "${sign_args[@]}" "$binary"
codesign --verify --strict --verbose=2 "$binary"

artifact_stem="skeindb-${version}-macos-${artifact_arch}"
codesign_log="$output_dir/${artifact_stem}-codesign.txt"
codesign -dv "$binary" >"$codesign_log" 2>&1

archive="$output_dir/${artifact_stem}.tar.gz"
tar -C "$(dirname "$binary")" -czf "$archive" "$(basename "$binary")"
shasum -a 256 "$archive" > "$archive.sha256"

echo "wrote $archive"
echo "wrote $archive.sha256"
echo "wrote $codesign_log"