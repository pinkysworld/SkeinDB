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

#[test]
fn skeinadmin_encryption_panel_exposes_key_management_controls() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");

    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for marker in [
        "data-panel=\"encryption\"",
        "Database Encryption",
        "btnEncStatus",
        "btnEncSetMode",
        "btnEncRegisterKey",
        "btnEncSetActive",
        "btnEncRotate",
        "encStatusGrid",
        "ENC_OFF",
        "ENC_RANDOM",
        "ENC_MLE_DB",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin encryption html should contain {marker}"
        );
    }

    for marker in [
        "settings.encryption.status",
        "settings.encryption.set_mode",
        "settings.encryption.register_key",
        "settings.encryption.set_active_key",
        "settings.encryption.rotate_key",
        "wire('btnEncStatus'",
        "wire('btnEncSetMode'",
        "wire('btnEncRegisterKey'",
        "wire('btnEncSetActive'",
        "wire('btnEncRotate'",
    ] {
        assert!(
            js.contains(marker),
            "skeinadmin encryption js should contain {marker}"
        );
    }
}

#[test]
fn skeinadmin_views_panel_exposes_dependency_usage_summary() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");

    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for marker in [
        "data-panel=\"views\"",
        "Incremental Views (R08)",
        "btnViewCreate",
        "btnViewRefresh",
        "btnViewStatus",
        "btnViewDrop",
        "btnViewExplainDeps",
        "viewSummary",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin views html should contain {marker}"
        );
    }

    for marker in [
        "function readViewRef()",
        "function renderViewSummary(result, mode)",
        "function renderViewDependency(dep)",
        "view.create',{view:readViewRef(),query}",
        "view.refresh',{view:readViewRef()}",
        "view.status',{view:readViewRef()}",
        "view.drop',{view:readViewRef()}",
        "view.explain_deps',{view:readViewRef()}",
    ] {
        assert!(
            js.contains(marker),
            "skeinadmin views js should contain {marker}"
        );
    }
}

#[test]
fn skeinadmin_dashboard_exposes_command_center_and_summary_bands() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");

    for marker in [
        "Operator Command Center",
        "Workspace Mission Control",
        "CDC Control Surface",
        "btnQuickCreateDb",
        "btnQuickCreateTable",
        "btnQuickInsertRow",
        "btnQuickBrowseData",
        "btnQuickCluster",
        "workspaceSummaryBar",
        "cdcSummaryBar",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin dashboard polish missing marker: {marker}"
        );
    }
}

#[test]
fn skeinadmin_panels_have_uniform_section_band_heroes() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");

    // Every major operator panel must lead with a section-band hero so the
    // console keeps a uniform dashboard-quality look.
    for hero_title in [
        "Workspace Mission Control",
        "Schema Command Deck",
        "Data Workbench",
        "Cluster Operations",
        "Settings &amp; Feature Flags",
        "Engine Configuration",
        "Telemetry &amp; Workload Insights",
        "Security Center",
        "Encryption Control",
        "CDC Control Surface",
        "Time Travel &amp; Replay",
        "Research Agenda Dashboard",
        "Vector Search",
        "Privacy Controls",
        "Forensic Audit Chain",
        "Incremental Views",
        "Merge &amp; CRDT Policies",
        "Wasm Query Operators",
        "Index Advisor",
        "Migration &amp; Compatibility",
        "Natural Language Lab",
        "RPC Explorer",
    ] {
        assert!(
            html.contains(hero_title),
            "skeinadmin panel hero missing: {hero_title}"
        );
    }

    // The cluster panel must group its previously-flat actions into observe /
    // enroll / shard-placement subsections so it stops feeling like a flat list.
    for marker in [">Observe<", ">Enroll Nodes<", ">Shard Placement<"] {
        assert!(
            html.contains(marker),
            "cluster panel should expose grouped action sections, missing: {marker}"
        );
    }
}

#[test]
fn skeinadmin_wires_research_runtime_surfaces() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let html_path = repo_root.join("web/skeinadmin/index.html");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");

    let html = fs::read_to_string(&html_path).expect("read web/skeinadmin/index.html");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for marker in [
        "Learned ValueID Index (R01)",
        "statLookupTotal",
        "statLearnedBuilt",
        "statLearnedSegments",
        "Compaction Scheduler (R20)",
        "energy_aware",
        "compactionPowerSource",
        "compactionPriceMultiplier",
        "btnCompactionStatus",
        "btnCompactionSavePolicy",
        "btnCompactionPause",
        "btnCompactionResume",
        "Edge Bundles (R14)",
        "btnEdgeRequest",
        "btnEdgeApply",
        "btnEdgeStatus",
        "edgeBundleJson",
    ] {
        assert!(
            html.contains(marker),
            "skeinadmin research html should contain {marker}"
        );
    }

    for marker in [
        "storage.learned_index",
        "statLookupTotal",
        "statLearnedBuilt",
        "async function compactionStatus()",
        "async function compactionSavePolicy()",
        "maintenance.compaction.status",
        "maintenance.compaction.set_policy",
        "external_signals",
        "estimated_joules_per_s",
        "maintenance.compaction.pause",
        "maintenance.compaction.resume",
        "async function edgeRequestBundle()",
        "async function edgeApplyBundle()",
        "async function edgeStatus()",
        "edge.bundle.request",
        "edge.bundle.apply",
        "edge.bundle.status",
        "wire('btnCompactionStatus', compactionStatus);",
        "wire('btnEdgeRequest', edgeRequestBundle);",
    ] {
        assert!(
            js.contains(marker),
            "skeinadmin research js should contain {marker}"
        );
    }
}

#[test]
fn skeinadmin_research_tracks_have_entry_points_for_all_twenty_items() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let js_path = repo_root.join("web/skeinadmin/src/main.js");
    let js = fs::read_to_string(&js_path).expect("read web/skeinadmin/src/main.js");

    for id in [
        "R01", "R02", "R03", "R04", "R05", "R06", "R07", "R08", "R09", "R10", "R11", "R12", "R13",
        "R14", "R15", "R16", "R17", "R18", "R19", "R20",
    ] {
        assert!(
            js.contains(&format!("id: '{id}'")),
            "missing research track {id}"
        );
    }

    for marker in [
        "panel: 'overview'",
        "panel: 'engine'",
        "panel: 'cluster'",
        "panel: 'workspace'",
        "panel: 'replay'",
        "panel: 'privacy'",
        "panel: 'forensics'",
        "panel: 'views'",
        "panel: 'merge'",
        "panel: 'wasm'",
        "panel: 'advisor'",
        "panel: 'migration'",
        "panel: 'nl'",
        "stats.snapshot",
        "query.patch",
        "maintenance.replay.export",
        "maintenance.compaction.status",
        "migration.report_export",
    ] {
        assert!(
            js.contains(marker),
            "research entry-point mapping should contain {marker}"
        );
    }
}
