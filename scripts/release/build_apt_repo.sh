#!/usr/bin/env bash

set -euo pipefail

deb_path=""
output_dir=""
suite="stable"
component="main"
arch="amd64"
origin="SkeinDB"
label="SkeinDB"
description="SkeinDB APT repository"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deb)
      deb_path="$2"
      shift 2
      ;;
    --output)
      output_dir="$2"
      shift 2
      ;;
    --suite)
      suite="$2"
      shift 2
      ;;
    --component)
      component="$2"
      shift 2
      ;;
    --arch)
      arch="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$deb_path" || -z "$output_dir" ]]; then
  echo "usage: build_apt_repo.sh --deb path/to/skeindb.deb --output dist/apt-repo" >&2
  exit 1
fi

if [[ ! -f "$deb_path" ]]; then
  echo "deb package not found: $deb_path" >&2
  exit 1
fi

rm -rf "$output_dir"

pool_rel="pool/$component/s/skeindb"
binary_rel="dists/$suite/$component/binary-$arch"
pool_dir="$output_dir/$pool_rel"
binary_dir="$output_dir/$binary_rel"
mkdir -p "$pool_dir" "$binary_dir"

deb_name="$(basename "$deb_path")"
cp "$deb_path" "$pool_dir/$deb_name"

pushd "$output_dir" >/dev/null

dpkg-scanpackages --multiversion pool /dev/null > "$binary_rel/Packages"
gzip -9c "$binary_rel/Packages" > "$binary_rel/Packages.gz"

cat > apt-ftparchive.conf <<EOF
APT::FTPArchive::Release::Origin "$origin";
APT::FTPArchive::Release::Label "$label";
APT::FTPArchive::Release::Suite "$suite";
APT::FTPArchive::Release::Codename "$suite";
APT::FTPArchive::Release::Architectures "$arch";
APT::FTPArchive::Release::Components "$component";
APT::FTPArchive::Release::Description "$description";
EOF

apt-ftparchive -c=apt-ftparchive.conf release "dists/$suite" > "dists/$suite/Release"
rm -f apt-ftparchive.conf

if [[ -n "${APT_GPG_PRIVATE_KEY:-}" && -n "${APT_GPG_KEY_ID:-}" ]]; then
  export GNUPGHOME
  GNUPGHOME="$(mktemp -d)"
  trap 'rm -rf "$GNUPGHOME"' EXIT

  printf '%s' "$APT_GPG_PRIVATE_KEY" | gpg --batch --import

  gpg_args=(--batch --yes --local-user "$APT_GPG_KEY_ID")
  if [[ -n "${APT_GPG_PASSPHRASE:-}" ]]; then
    gpg_args+=(--pinentry-mode loopback --passphrase "$APT_GPG_PASSPHRASE")
  fi

  gpg "${gpg_args[@]}" --armor --output public.key --export "$APT_GPG_KEY_ID"
  gpg "${gpg_args[@]}" --output pubkey.gpg --export "$APT_GPG_KEY_ID"
  gpg "${gpg_args[@]}" --output "dists/$suite/InRelease" --clearsign "dists/$suite/Release"
  gpg "${gpg_args[@]}" --output "dists/$suite/Release.gpg" --detach-sign "dists/$suite/Release"
else
  echo "APT signing secrets are not configured; generated an unsigned repository only." >&2
fi

popd >/dev/null