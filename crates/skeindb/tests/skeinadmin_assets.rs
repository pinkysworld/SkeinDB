use std::fs;
use std::path::PathBuf;

#[test]
fn skeinadmin_forensics_panel_exposes_audit_controls() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");

    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for marker in [
        "Audit Chain Health",
        "btnForAuditStatus",
        "btnForAuditVerify",
        "forAuditSummary",
        "Proof Verify",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin forensics html should contain {marker}"
        );
    }

    for marker in [
        "async function forAuditStatus()",
        "async function forAuditVerify()",
        "maintenance.audit_status",
        "maintenance.audit_verify",
        "wire('btnForAuditStatus', forAuditStatus);",
        "wire('btnForAuditVerify', forAuditVerify);",
    ] {
        assert!(
            js.contains(marker),
            "skeinadmin forensics js should contain {marker}"
        );
    }
}

#[test]
fn skeinadmin_cdc_panel_exposes_subscription_controls() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");

    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for marker in [
        "data-panel=\"cdc\"",
        "CDC Subscriptions",
        "btnCdcSubscribe",
        "btnCdcPoll",
        "btnCdcAck",
        "btnCdcClose",
        "cdcLagSummary",
        "cdcEventGrid",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin cdc html should contain {marker}"
        );
    }

    for marker in [
        "async function cdcSubscribe()",
        "async function cdcPoll()",
        "async function cdcAck()",
        "async function cdcClose()",
        "function renderCdcPanel()",
        "wire('btnCdcSubscribe', cdcSubscribe);",
        "wire('btnCdcPoll', cdcPoll);",
        "wire('btnCdcAck', cdcAck);",
        "wire('btnCdcClose', cdcClose);",
    ] {
        assert!(
            js.contains(marker),
            "skeinadmin cdc js should contain {marker}"
        );
    }
}

#[test]
fn skeinadmin_replay_panel_exposes_time_travel_and_integrity_controls() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");

    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for marker in [
        "data-panel=\"replay\"",
        "Point-in-Time Query Runner",
        "btnTimeTravelSeed",
        "btnTimeTravelRun",
        "btnHistoryStatus",
        "btnHistorySetPolicy",
        "btnHistoryGc",
        "btnReplayExport",
        "btnReplayImport",
        "btnReplayRunIntegrity",
        "replayBundleSummary",
        "replayIntegritySummary",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin replay html should contain {marker}"
        );
    }

    for marker in [
        "function renderReplayPanel()",
        "async function timeTravelRunQuery()",
        "async function historyLoadStatus()",
        "async function historySavePolicy()",
        "async function historyRunGc()",
        "async function replayExportBundle()",
        "async function replayImportBundle()",
        "async function replayRunIntegrity()",
        "maintenance.history.status",
        "maintenance.history.set_policy",
        "maintenance.history.gc",
        "maintenance.replay.export",
        "maintenance.replay.import",
        "maintenance.replay.run",
        "wire('btnTimeTravelRun', timeTravelRunQuery);",
        "wire('btnReplayRunIntegrity', replayRunIntegrity);",
    ] {
        assert!(
            js.contains(marker),
            "skeinadmin replay js should contain {marker}"
        );
    }
}

#[test]
fn skeinadmin_easy_design_tab_exposes_wysiwyg_schema_editor() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");

    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for marker in [
        "data-etab=\"design\"",
        "easyDesignLoad",
        "easyDesignAddCol",
        "easyDesignReset",
        "easyDesignPreview",
        "easyDesignApply",
        "easyDesignRows",
        "easyDesignStatus",
        "Planned ALTER statements",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin design html should contain {marker}"
        );
    }

    for ma in [
        "async function easyDesignLoad()",
        "function easyDesignBuildAlterPlan()",
        "function easyDesignAddColumn()",
        "async function easyDesignApply()",
        "ALTER TABLE",
        "DROP COLUMN",
        "ADD COLUMN",
        "RENAME COLUMN",
        "MODIFY COLUMN",
        "CHANGE COLUMN",
        "wire('easyDesignLoad', easyDesignLoad);",
        "wire('easyDesignApply', easyDesignApply);",
    ] {
        assert!(js.contains(ma), "skeinadmin design js should contain {ma}");
    }
}
