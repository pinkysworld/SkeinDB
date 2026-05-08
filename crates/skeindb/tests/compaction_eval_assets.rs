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
fn compaction_eval_harness_generates_summary_and_dashboard() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let script_path = repo_root.join("eval/compaction_scheduler_dashboard.py");
    let docs_path = repo_root.join("docs/COMPACTION_SCHEDULER.md");

    let script =
        fs::read_to_string(&script_path).expect("read eval/compaction_scheduler_dashboard.py");
    let docs = fs::read_to_string(&docs_path).expect("read docs/COMPACTION_SCHEDULER.md");

    for marker in [
        "compaction_scheduler_summary.json",
        "compaction_scheduler_timeline.csv",
        "compaction_scheduler_dashboard.html",
        "fixed_leveling",
        "fixed_tiering",
        "workload_guided",
        "energy_aware",
        "stall_rate",
        "write_p99_ms_peak",
        "read_p99_ms_peak",
        "energy_joules_total",
    ] {
        assert!(
            script.contains(marker),
            "compaction eval script should contain {marker}"
        );
    }

    assert!(
        docs.contains("compaction_scheduler_dashboard.py"),
        "compaction scheduler docs should reference the evaluation harness"
    );

    let Some(python) = python_command() else {
        eprintln!("python interpreter not found; static asset checks completed");
        return;
    };

    let outdir = temp_dir("compaction_eval_dashboard");
    let status = Command::new(python)
        .arg(&script_path)
        .arg("--scenario")
        .arg("mixed")
        .arg("--seconds")
        .arg("180")
        .arg("--seed")
        .arg("7")
        .arg("--outdir")
        .arg(&outdir)
        .status()
        .expect("run compaction eval harness");
    assert!(status.success(), "compaction eval harness should succeed");

    let summary = fs::read_to_string(outdir.join("compaction_scheduler_summary.json"))
        .expect("read generated summary json");
    let dashboard = fs::read_to_string(outdir.join("compaction_scheduler_dashboard.html"))
        .expect("read generated dashboard html");
    let csv = fs::read_to_string(outdir.join("compaction_scheduler_timeline.csv"))
        .expect("read generated timeline csv");

    for marker in [
        "\"stall_rate\"",
        "\"write_p99_ms_peak\"",
        "\"read_p99_ms_peak\"",
        "\"workload_guided\"",
        "\"energy_aware\"",
        "\"energy_joules_total\"",
    ] {
        assert!(
            summary.contains(marker),
            "generated summary should contain {marker}"
        );
    }

    for marker in [
        "Compaction Scheduler Dashboard",
        "Stall Rate Comparison",
        "P99 Write Latency Over Time",
        "P99 Read Latency Over Time",
        "Energy Score Total",
    ] {
        assert!(
            dashboard.contains(marker),
            "generated dashboard should contain {marker}"
        );
    }

    assert!(
        csv.lines()
            .next()
            .unwrap_or_default()
            .contains("policy,second,phase"),
        "timeline csv should contain the expected header"
    );

    fs::remove_dir_all(outdir).ok();
}
