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
