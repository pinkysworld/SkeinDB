# SkeinDB On-Disk Format v0.3 (v0.2 compatible)

Status: Draft v0.3 (v0.2 compatible)
Last updated: 2026-02-24

This document defines SkeinDB's on-disk storage layout and record formats.
All formats MUST be versioned. Any breaking change requires a format version bump.

Design goals:
- Append-only segments
- Crash safety with WAL + checkpoint
- MVCC row versioning
- Optional deduplicated ValueStore (content-addressed)
- Simple compaction + GC suitable for single-binary deployments

---

## 1) Directory layout

data/
  MANIFEST.log
  MANIFEST.snapshot            (optional)
  snapshots.json              (prototype snapshot metadata, format v1)
  dp_budgets.json             (prototype DP budgets, format v1)
  dp_audit.json               (prototype DP audit log, format v1)
  oblivious_policies.json     (prototype oblivious policy store, format v1)
  forensic_chain.json         (prototype forensic hash chain, format v1)
  merge_policies.json         (prototype merge policies, format v1)
  merge_wasm_registry.json    (prototype merge wasm registry, format v1)
  views.json                  (prototype materialized views, format v1)
  schema_versions.json        (prototype schema versions, format v1)
  schema_changes.json         (prototype schema change log, format v1)
  advisor_patterns.json       (prototype index advisor patterns, format v1)
  advisor_history.json        (prototype index advisor history, format v1)
  wal/
    wal-000001.log
  rows/
    rows-000001.rseg
  vals/
    vals-000001.vseg
  idx/
    rowdir-L0-000001.run
    valdir-L0-000002.run
    pk_<table>-L0-000003.run
  tmp/

---

## 2) Common encodings

### 2.1 Endianness
- Fixed-width integers in headers and record bodies are LITTLE-ENDIAN.

### 2.2 VarU (ULEB128)
- Variable-length unsigned integer encoding for u64.
- 7 bits payload per byte, MSB continuation.

### 2.3 Bytes and String
- Bytes: VarU length + N bytes
- String: VarU length + UTF-8 bytes

### 2.4 Checksums
- CRC32C over record payload bytes (not including len/crc).

### 2.5 ValueID
- ValueID = BLAKE3-128(value_bytes) (16 bytes)
- On lookup, verify bytes equality to eliminate collision risk.

---

## 3) File header (64 bytes)

FileHeader (64 bytes)
  magic[8]        = ASCII "SKNDB\0\1"
  file_kind       = u8  (1=wal,2=rowseg,3=valseg,4=run,5=manifest)
  endian          = u8  (1=little)
  header_len      = u16 (64)
  format_ver      = u32 (1)
  file_id         = u32 (segment/run id)
  created_unix_s  = u64
  reserved[32]    = bytes (0)
  header_crc32c   = u32 (CRC32C over bytes 0..59)

---

## 4) Record framing

RecordFrame:
  len      u32 (LE)
  crc32c   u32 (LE)
  payload  [len] bytes

---

## 5) Pointers

FilePtr (12 bytes):
  file_id  u32
  offset   u64

---

## 6) Row segments (.rseg)

RV1 record payload:

RV1
  rec_type     u8   = 0x10
  rec_ver      u8   = 1
  flags        u16
  table_id     u32
  row_id       u64
  begin_ts     u64      // commit_ts; 0 allowed only in WAL staging
  end_ts       u64      // 0 means +INF
  prev_ptr     FilePtr  // previous row version (or 0/0)
  group_count  VarU

  repeated group_count:
    group_id        VarU
    group_ref_kind  u8   (0=inline_small, 1=value_id_ref)
    if kind=0:
      group_bytes   Bytes     // GroupObject bytes (GO1)
    if kind=1:
      group_vid[16]           // ValueID of a GROUP in value store

Flags:
- bit0 IS_DELETE

---

## 7) Value segments (.vseg)

VE1 record payload:

VE1
  rec_type     u8  = 0x20
  rec_ver      u8  = 1
  val_kind     u8  (1=CELL, 2=GROUP, 3=BLOB_CHUNK, 4=BLOB_MANIFEST, 5=DELTA, 6=EMBEDDING)
  codec        u8  (0=RAW, 1=ZSTD)
  value_id[16]
  raw_len      VarU
  raw_bytes    Bytes-or-compressed

GroupObject bytes GO1:
- See v0.1 GO1 spec (GroupObject is the dedup unit for a group of columns)

---

## 8) Sorted runs (.run)

A .run is an immutable sorted key/value table (SSTable-like), used for:
- rowdir: row_id -> FilePtr
- valdir: value_id -> FilePtr
- primary/secondary indexes

DataBlock payload:
  block_type  u8 = 0x40
  block_ver   u8 = 1
  entry_count VarU
  repeated entry_count:
    key Bytes
    value Bytes

IndexBlock payload:
  block_type u8 = 0x41
  block_ver  u8 = 1
  block_count VarU
  repeated:
    first_key Bytes
    block_offset u64

Footer:
  footer_magic[8] = "SKNRUN\0\1"
  index_offset u64
  file_crc32c u32 (optional)

---

## 9) WAL (.log)

WALHeader prefix for all WAL records:

WALHeader
  rec_type u8
  rec_ver  u8
  flags    u16
  lsn      u64
  txn_id   u64

Commit rule:
- A txn is committed iff a valid COMMIT_TXN record exists.
- Recovery replays only committed txns in LSN order.

---

## 10) Compaction and GC

- Compute safe_ts = oldest_active_snapshot_ts.
- Row compaction discards versions with end_ts < safe_ts.
- Value GC is mark-and-sweep driven by live row versions.

---

## 11) Prototype metadata JSON (format v1)

These JSON files are optional and may be ignored by older binaries.
Each file includes a `format_version` field; unknown versions should be ignored.

### 11.1 merge_wasm_registry.json

Format:

```json
{
  "format_version": 1,
  "modules": [
    {
      "module_id": "merge_sum",
      "value_id": "deadbeef...",
      "size_bytes": 1234,
      "capabilities": {
        "values_only": true,
        "deterministic": true,
        "max_fuel": 1000,
        "max_memory_bytes": 65536,
        "max_output_bytes": 4096
      },
      "name": "sum merge",
      "wasm_b64": "AA==",
      "created_at_ms": 1730000000000
    }
  ]
}
```

Compatibility notes:
- Added in v0.2 as an optional metadata file.
- If the file is missing or has an unknown `format_version`, it is ignored.

### 11.2 tables/<db>/<table>.json (format v2)

Prototype row persistence for `tables/<db>/<table>.json` now supports a
ValueID-backed JSON format to reduce duplicated literal payloads in row files.

Format:

```json
{
  "format_version": 2,
  "rows": [
    {
      "row": {
        "id": {"t":"u64","v":1},
        "payload": {
          "$skein_ref": {
            "kind": "cell",
            "id": "0123abcd...32hex",
            "lit": {"t":"str","v":"hello"}
          }
        }
      },
      "version": 1,
      "deleted": false
    },
    {
      "row": {
        "id": {"t":"u64","v":2},
        "payload": {
          "$skein_ref": {
            "kind": "cell",
            "id": "0123abcd...32hex"
          }
        }
      },
      "version": 2,
      "deleted": false
    }
  ]
}
```

Rules:
- `"$skein_ref".id` is a 32-char hex ValueID.
- The first occurrence of a ValueID in a table file should include `lit` seed data.
- Later duplicates may omit `lit` and reference only `id`.
- Unknown `format_version` values are treated as unsupported and should fall back to legacy readers.
- v0.1/v0.2 legacy row arrays (`Vec<RowEntry>`) remain readable.

### 11.3 tables/<db>/<table>.rseg (prototype segment container v1)

SkeinDB can also persist table rows in a compact framed container with extension `.rseg`.

Header:
- `magic[8]`: `SKNSEGR1`
- `segment_format_version` (`u32 LE`): currently `1`
- `table_format_version` (`u32 LE`): currently `2` (same row payload schema as `.json`)
- `row_count` (`u64 LE`)

Body:
- Repeated `row_count` times:
  - `payload_len` (`u32 LE`)
  - `payload` (`payload_len` bytes) as JSON-encoded `RowEntryDisk`

Behavior:
- `SKEINDB_STORAGE_MODE=json` (default): write/read `.json`; fallback read `.rseg`.
- `SKEINDB_STORAGE_MODE=segment`: write/read `.rseg`; fallback read `.json`.
- `SKEINDB_STORAGE_MODE=dual`: write both formats; read prefers `.rseg`, then `.json`.

Compatibility notes:
- Unsupported segment header versions are ignored by fallback readers.
- If both files are missing or unreadable, the table loads as empty.

---

# Appendix A) v0.2/v0.3 extensions

This appendix specifies optional extensions that can be implemented without invalidating v0.1 data.
The FileHeader format_ver remains 1; extensions use new record types and/or higher rec_ver values.

## A.1 Delta ValueEntries

Value segments (.vseg) add a new value kind:
- val_kind = 5 (DELTA)

DELTA entries store a patch against a base ValueID. See docs/DELTA_VALUES.md.

Suggested DELTA payload (VE1-compatible by treating raw_bytes as a DELTA1 container):

DELTA1 (stored inside VE1 raw_bytes):
- base_vid[16]
- delta_codec u8
- full_len VarU
- patch_bytes Bytes

Readers that do not understand DELTA should treat it as unsupported.

## A.2 Hash-chained WAL records

WALHeader v1 (rec_ver=1) has:
- rec_type u8
- rec_ver u8
- flags u16
- lsn u64
- txn_id u64

WALHeader v2 (rec_ver=2) extends v1 by appending:
- prev_hash[32]
- rec_hash[32]

Hash rules are defined in docs/AUDIT_WAL.md.

This approach does not require changing the WAL FileHeader.

## A.3 Column snapshots

Add a new directory:

  snapshots/

Snapshot files are independent from WAL/rows/vals and can be deleted/rebuilt.

Suggested snapshot file kind:
- file_kind = 6 (snapshot)

Prototype metadata (scaffold):
- `snapshots.json` stores column snapshot metadata + row values.
- JSON includes `format_version` (current: 1) and a per-snapshot `table_version`.
- On startup, snapshots are loaded only when `table_version` matches the catalog.

Within snapshots/, column segments may use their own header format.
See docs/COLUMN_SNAPSHOTS.md for cseg v0.1.

## A.4 Index advisor telemetry (prototype)

- `advisor_patterns.json` stores aggregated query dependency patterns (format v1).
- `advisor_history.json` stores apply/dismiss actions (format v1).

Files are optional and written only when `SKEINDB_ADVISOR_PERSIST=1`.

## A.5 Embedding ValueEntries

Value segments (.vseg) add a new value kind:
- val_kind = 6 (EMBEDDING)

Embedding entries store vector values plus an optional model identifier.

ValueID semantics (embedding-only):
- ValueID[0..8] = LSH bucket (u64 LE)
- ValueID[8..16] = first 8 bytes of BLAKE3-128 over EMB1 bytes

Suggested EMB1 payload (VE1 raw_bytes):

EMB1:
- magic[4] = "EMB1"
- dims u32 (LE)
- model_len VarU
- model_bytes Bytes (UTF-8, length = model_len; may be 0)
- values f32[dims] (LE)

Readers that do not understand EMBEDDING should treat it as unsupported.
