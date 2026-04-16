class Skeindb < Formula
  desc "Single-binary database server with SkeinQL, SkeinAdmin, MySQL, and PostgreSQL surfaces"
  homepage "https://github.com/pinkysworld/SkeinDB"
  head "https://github.com/pinkysworld/SkeinDB.git", branch: "main"
  license "Apache-2.0"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/skeindb")
  end

  test do
    assert_match "serve", shell_output("#{bin}/skeindb --help")
  end
end