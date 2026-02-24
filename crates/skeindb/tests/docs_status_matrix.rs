use std::fs;
use std::path::PathBuf;

#[test]
fn true_status_matrix_links_backlogs_and_research_tracks() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let matrix_path = repo_root.join("docs/TRUE_STATUS_MATRIX.md");
    let matrix = fs::read_to_string(&matrix_path).expect("read docs/TRUE_STATUS_MATRIX.md");

    assert!(
        matrix.contains("docs/PROJECT_BACKLOG.md"),
        "matrix should reference docs/PROJECT_BACKLOG.md"
    );
    assert!(
        matrix.contains("docs/RESEARCH_BACKLOG.md"),
        "matrix should reference docs/RESEARCH_BACKLOG.md"
    );

    for track in 1..=20 {
        let marker = format!("R{:02}", track);
        assert!(
            matrix.contains(&marker),
            "matrix should mention research track marker {marker}"
        );
    }
}
