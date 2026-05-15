use std::path::PathBuf;
use std::process::Command;

#[test]
fn vector_rag_sample_self_test_runs() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let sample_path = repo_root.join("samples/vector_rag_pipeline.py");
    assert!(sample_path.exists(), "missing vector RAG sample app");

    let output = Command::new("python3")
        .arg(&sample_path)
        .arg("--self-test")
        .output()
        .expect("run vector RAG sample self-test");

    assert!(
        output.status.success(),
        "sample self-test failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\": true"));
    assert!(stdout.contains("\"dims\": 8"));
}

#[test]
fn vector_rag_sample_uses_native_vector_rpc_flow() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let sample_path = repo_root.join("samples/vector_rag_pipeline.py");
    let sample = std::fs::read_to_string(sample_path).expect("read vector RAG sample app");

    for marker in [
        "schema.create_table",
        "data.insert",
        "vector.insert",
        "vector.search",
        "include_row",
        "toy_embedding",
        "build_prompt",
        "model\": \"toy-hash-v1",
    ] {
        assert!(
            sample.contains(marker),
            "vector RAG sample should contain {marker}"
        );
    }
}
