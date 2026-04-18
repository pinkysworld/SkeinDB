#!/usr/bin/env python3
"""Render the SkeinDB Homebrew formula for a tagged release."""

from __future__ import annotations

import argparse
from pathlib import Path


FORMULA_TEMPLATE = r'''class Skeindb < Formula
  desc "Single-binary database server with SkeinQL, SkeinAdmin, MySQL, and PostgreSQL surfaces"
  homepage "https://github.com/pinkysworld/SkeinDB"
  url "https://github.com/pinkysworld/SkeinDB/releases/download/v__VERSION__/skeindb-__VERSION__-source.tar.gz"
  sha256 "__SHA256__"
  head "https://github.com/pinkysworld/SkeinDB.git", branch: "main"
  license "Apache-2.0"

  livecheck do
    url :stable
    strategy :github_latest
  end

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/skeindb")
  end

  def post_install
    (var/"skeindb").mkpath
  end

  service do
    run [opt_bin/"skeindb", "serve", "--data", var/"skeindb", "--http", "8080",
         "--mysql", "3306", "--pg", "5432", "--bind", "127.0.0.1"]
    keep_alive true
    working_dir var/"skeindb"
    log_path var/"log/skeindb.log"
    error_log_path var/"log/skeindb.log"
  end

  def caveats
    <<~EOS
      Data is stored in #{var}/skeindb by default.

      To start SkeinDB now and restart at login:
        brew services start skeindb

      The default service listens on:
        HTTP/SkeinAdmin : http://127.0.0.1:8080/admin
        MySQL           : 127.0.0.1:3306
        PostgreSQL      : 127.0.0.1:5432

      If you have MySQL or PostgreSQL already installed, disable the
      conflicting listeners by running manually instead:
        skeindb serve --data #{var}/skeindb --mysql 0 --pg 0
    EOS
  end

  test do
    port = free_port
    data_dir = testpath/"data"

    pid = fork do
      exec bin/"skeindb", "serve",
           "--data", data_dir,
           "--http", port.to_s,
           "--mysql", "0",
           "--pg", "0",
           "--bind", "127.0.0.1"
    end

    sleep 2

    begin
      output = shell_output("curl -sf http://127.0.0.1:#{port}/health")
      assert_equal "ok", output

      rpc = shell_output(
        "curl -sf -X POST http://127.0.0.1:#{port}/api/v1/rpc " \
        "-H 'Content-Type: application/json' " \
        "-d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"system.ping\",\"params\":{}}'" \
      )
      assert_match "pong", rpc
    ensure
      Process.kill("TERM", pid)
      Process.wait(pid)
    end
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
        FORMULA_TEMPLATE.replace("__VERSION__", args.version).replace("__SHA256__", args.sha256),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()