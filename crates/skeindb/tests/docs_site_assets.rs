use std::fs;
use std::path::PathBuf;

#[test]
fn architecture_figure_path_resolves() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let site_index = repo_root.join("docs/site/index.html");
    let html = fs::read_to_string(&site_index).expect("read docs/site/index.html");

    let section_start = html
        .find("id=\"architecture\"")
        .expect("architecture section missing");
    let section = &html[section_start..];

    let src_marker = "img src=\"";
    let src_start = section
        .find(src_marker)
        .expect("architecture image missing");
    let src = &section[src_start + src_marker.len()..];
    let src_end = src.find('"').expect("architecture image src malformed");
    let rel_src = &src[..src_end];

    let rel_path = rel_src.split('#').next().unwrap_or(rel_src);
    let image_path = site_index
        .parent()
        .expect("site index parent")
        .join(rel_path);
    assert!(
        image_path.exists(),
        "architecture image path does not exist: {}",
        image_path.display()
    );
}

#[test]
fn vector_rag_tutorial_is_linked_and_generated() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = repo_root.join("docs/tutorials/vector-rag.md");
    let generated = repo_root.join("docs/site/guide/vector-rag.html");
    let guide_index = repo_root.join("docs/site/guide/index.html");

    let source_md = fs::read_to_string(&source).expect("read vector RAG tutorial source");
    for marker in [
        "samples/vector_rag_pipeline.py",
        "vector.insert",
        "vector.search",
        "vector.benchmark",
        "toy-hash-v1",
    ] {
        assert!(
            source_md.contains(marker),
            "vector RAG tutorial source should contain {marker}"
        );
    }

    let generated_html = fs::read_to_string(&generated).expect("read generated vector RAG guide");
    assert!(generated_html.contains("Vector RAG retrieval"));
    assert!(generated_html.contains("vector.search"));
    assert!(generated_html.contains("samples/vector_rag_pipeline.py"));

    let guide_index_html = fs::read_to_string(&guide_index).expect("read generated guide index");
    assert!(guide_index_html.contains("vector-rag.html"));
    assert!(guide_index_html.contains("Vector RAG retrieval"));
}
