//! Storage mode parsing and the decision helpers that route row persistence,
//! extracted from the monolithic engine to keep mode logic in one place.

use std::path::Path;

use skeindb_core::manifest::ManifestReader;

const STORAGE_MODE_ENV: &str = "SKEINDB_STORAGE_MODE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableStorageMode {
    Json,
    Segment,
    Dual,
}

impl TableStorageMode {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "segment" => Some(Self::Segment),
            "dual" | "hybrid" => Some(Self::Dual),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Segment => "segment",
            Self::Dual => "hybrid",
        }
    }

    pub(crate) fn from_env() -> Self {
        let raw = std::env::var(STORAGE_MODE_ENV).unwrap_or_default();
        Self::parse(&raw).unwrap_or(Self::Dual)
    }

    /// Returns true for modes that use the .rseg (segment) row files as primary/dual path.
    /// Part of monolith split (reviewer rec B): decision logic lives with the mode enum
    /// rather than duplicated in engine.rs / server.rs.
    pub(crate) fn uses_segment(&self) -> bool {
        matches!(self, Self::Segment | Self::Dual)
    }

    /// Returns true for modes where core MANIFEST/WAL/LSM pipeline files are expected
    /// alongside the row segments. Segment/Dual use `.rseg`; Json never expects core
    /// LSM files. Backs the `core_lsm_files_active` storage observability check.
    pub(crate) fn expects_core_lsm_files(&self) -> bool {
        self.uses_segment()
    }
}

/// Returns whether the core LSM pipeline files (`MANIFEST.log` plus at least one
/// `wal-*.log`) are present in `data_dir` for the given storage mode. Json mode
/// never uses them; segment/dual modes expect them alongside the `.rseg` files.
/// Backs the `core_lsm_files_active` storage observability stat.
pub(crate) fn lsm_pipeline_files_active(data_dir: &Path, mode: TableStorageMode) -> bool {
    if !mode.expects_core_lsm_files() {
        return false;
    }
    let manifest_path = data_dir.join("MANIFEST.log");
    if ManifestReader::open(&manifest_path).is_err() {
        return false;
    }
    // Look for at least one wal file produced by the core WAL writer.
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("wal-") && name.ends_with(".log") {
                    return true;
                }
            }
        }
    }
    false
}
