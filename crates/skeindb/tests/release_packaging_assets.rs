use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn python_command() -> Option<&'static str> {
    for candidate in ["python3", "python"] {
        let ok = Command::new(candidate)
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        if ok {
            return Some(candidate);
        }
    }
    None
}

fn temp_dir(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("skeindb_{name}_{}_{}", std::process::id(), suffix));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn release_packaging_assets_cover_apt_and_homebrew() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let gitignore = fs::read_to_string(repo_root.join(".gitignore")).expect("read .gitignore");
    let cargo =
        fs::read_to_string(repo_root.join("crates/skeindb/Cargo.toml")).expect("read cargo");
    let workflow = fs::read_to_string(repo_root.join(".github/workflows/release-packages.yml"))
        .expect("read workflow");
    let formula = fs::read_to_string(repo_root.join("Formula/skeindb.rb")).expect("read formula");
    let apt_script = fs::read_to_string(repo_root.join("scripts/release/build_apt_repo.sh"))
        .expect("read apt repo script");
    let formula_script =
        fs::read_to_string(repo_root.join("scripts/release/render_homebrew_formula.py"))
            .expect("read formula render script");
    let readme = fs::read_to_string(repo_root.join("README.md")).expect("read readme");
    let getting_started = fs::read_to_string(repo_root.join("docs/GETTING_STARTED.md"))
        .expect("read getting started");
    let backlog =
        fs::read_to_string(repo_root.join("docs/PROJECT_BACKLOG.md")).expect("read backlog");
    let status = fs::read_to_string(repo_root.join("docs/TRUE_STATUS_MATRIX.md"))
        .expect("read status matrix");

    for marker in ["target_*/", "dist/"] {
        assert!(
            gitignore.contains(marker),
            ".gitignore should contain {marker}"
        );
    }

    for marker in [
        "description = \"Single-binary database server",
        "homepage = \"https://github.com/pinkysworld/SkeinDB\"",
        "[package.metadata.deb]",
        "target/release/skeindb",
    ] {
        assert!(
            cargo.contains(marker),
            "cargo metadata should contain {marker}"
        );
    }

    for marker in [
        "cargo install cargo-deb --locked",
        "softprops/action-gh-release@v2",
        "render_homebrew_formula.py",
        "build_apt_repo.sh",
        "git push --force origin apt",
    ] {
        assert!(
            workflow.contains(marker),
            "workflow should contain {marker}"
        );
    }

    for marker in [
        "head \"https://github.com/pinkysworld/SkeinDB.git\", branch: \"main\"",
        "depends_on \"rust\" => :build",
        "crates/skeindb",
    ] {
        assert!(formula.contains(marker), "formula should contain {marker}");
    }

    for marker in [
        "releases/download/v{version}/skeindb-{version}-source.tar.gz",
        "std_cargo_args(path: \"crates/skeindb\")",
    ] {
        assert!(
            formula_script.contains(marker),
            "formula render script should contain {marker}"
        );
    }

    for marker in [
        "dpkg-scanpackages",
        "apt-ftparchive",
        "public.key",
        "pubkey.gpg",
        "InRelease",
    ] {
        assert!(
            apt_script.contains(marker),
            "apt script should contain {marker}"
        );
    }

    for marker in [
        "brew tap pinkysworld/skeindb",
        "brew install --HEAD pinkysworld/skeindb/skeindb",
        "apt-get install skeindb",
    ] {
        assert!(readme.contains(marker), "README should contain {marker}");
        assert!(
            getting_started.contains(marker),
            "getting started should contain {marker}"
        );
    }

    assert!(backlog.contains("## Phase 26 - Distribution and installation"));
    assert!(backlog.contains("T419"));
    assert!(backlog.contains("T420"));
    assert!(status.contains("Phase 26 Distribution"));

    let Some(python) = python_command() else {
        eprintln!("python interpreter not found; static packaging checks completed");
        return;
    };

    let outdir = temp_dir("release_packaging_formula");
    let output = outdir.join("skeindb.rb");
    let status = Command::new(python)
        .arg(repo_root.join("scripts/release/render_homebrew_formula.py"))
        .arg("--version")
        .arg("0.3.0")
        .arg("--sha256")
        .arg("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        .arg("--output")
        .arg(&output)
        .status()
        .expect("run formula render script");
    assert!(status.success(), "formula render script should succeed");

    let rendered = fs::read_to_string(output).expect("read rendered formula");
    assert!(
        rendered.contains(
            "url \"https://github.com/pinkysworld/SkeinDB/releases/download/v0.3.0/skeindb-0.3.0-source.tar.gz\""
        ),
        "rendered formula should point at the tagged release asset"
    );
    assert!(
        rendered.contains(
            "sha256 \"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\""
        ),
        "rendered formula should contain the requested sha256"
    );

    fs::remove_dir_all(outdir).ok();
}
