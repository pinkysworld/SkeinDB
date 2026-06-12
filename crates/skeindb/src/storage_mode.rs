//! Storage mode handling extracted from the monolithic engine.
//! Part of monolith split effort (reviewer recommendation B) to improve maintainability
//! for all future hardening work on partial areas.

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
    /// (alongside or instead of prototype table snapshots). Centralizes mode decision
    /// in the extracted storage_mode module (B monolith split).
    /// Delegates to uses_segment for now (Segment/Dual); Json never expects core LSM files.
    /// Used by engine core_lsm_files_active observability (A storage pipeline micro).
    pub(crate) fn expects_core_lsm_files(&self) -> bool {
        self.uses_segment()
    }

    /// Primary on-disk extension used for row data under this mode (for docs/observability).
    #[allow(dead_code)]
    pub(crate) fn primary_row_extension(&self) -> &'static str {
        if self.uses_segment() {
            "rseg"
        } else {
            "json"
        }
    }
}
