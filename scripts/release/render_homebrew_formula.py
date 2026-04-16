#!/usr/bin/env python3
"""Render the SkeinDB Homebrew formula for a tagged release."""

from __future__ import annotations

import argparse
from pathlib import Path


FORMULA_TEMPLATE = '''class Skeindb < Formula
  desc "Single-binary database server with SkeinQL, SkeinAdmin, MySQL, and PostgreSQL surfaces"
  homepage "https://github.com/pinkysworld/SkeinDB"
  url "https://github.com/pinkysworld/SkeinDB/releases/download/v{version}/skeindb-{version}-source.tar.gz"
  sha256 "{sha256}"
  head "https://github.com/pinkysworld/SkeinDB.git", branch: "main"
  license "Apache-2.0"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/skeindb")
  end

  test do
    assert_match "serve", shell_output("#{{bin}}/skeindb --help")
  end
end
'''


def main() -> None:
    parser = argparse.ArgumentParser(description="Render Formula/skeindb.rb")
    parser.add_argument("--version", required=True, help="Release version without the leading v")
    parser.add_argument("--sha256", required=True, help="SHA256 for the release source tarball")
    parser.add_argument("--output", required=True, help="Output path for the rendered formula")
    args = parser.parse_args()

    output = Path(args.output)
    output.write_text(
        FORMULA_TEMPLATE.format(version=args.version, sha256=args.sha256),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()