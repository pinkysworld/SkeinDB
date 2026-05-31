# SkeinDB On-Disk Format v0.3 (v0.2 compatible)

Status: Draft v0.3 (v0.2 compatible)
Last updated: 2026-05-27

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
  forensic_chain.json         (prototype forensic hash chain, format v3)
  wasm_catalog.json           (prototype Wasm UDF catalog, format v1)
  merge_policies.json         (prototype merge policies, format v1)
  merge_wasm_registry.json    (prototype merge wasm registry, format v1)
  views.json                  (prototype materialized views, format v2)
  schema_versions.json        (prototype schema versions, format v1)
  schema_changes.json         (prototype schema change log, format v2)
  schema_flags.json           (prototype schema flags, format v1)
  changes.json                (prototype retained CDC change log, format v2)
  cdc_subscriptions.json      (prototype CDC subscription cursors, format v9)
  advisor_patterns.json       (prototype index advisor patterns, format v1)
  advisor_history.json        (prototype index advisor history, format v2)
  security_state.json         (security principals + API tokens, format v1)
  tables/
    <db>/<table>.json         (prototype row store, format v3)
    <db>/<table>.rseg         (prototype row segment container, format v1)
    <db>/<table>.sidx.json    (prototype secondary index cache, format v1)
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

### 4.1 MANIFEST.log payload records (format v1)

Each MANIFEST record is wrapped in a `RecordFrame`. The payload body starts
with a `rec_type` tag, then uses VarU fields.

ManifestRecordV1 payloads:

- `rec_type = 0x01` (`AddFile`)
  - `file_kind` u8 (`1=wal,2=rowseg,3=valseg,4=run`)
  - `file_id` VarU (u32 domain)
  - `level` VarU (u32 domain)
- `rec_type = 0x02` (`RemoveFile`)
  - `file_kind` u8
  - `file_id` VarU (u32 domain)
- `rec_type = 0x03` (`SetCurrentVersion`)
  - `version` VarU (u64)
- `rec_type = 0x04` (`SetLastLsn`)
  - `lsn` VarU (u64)
- `rec_type = 0x05` (`CleanShutdown`)
  - `unix_s` VarU (u64)

Replayed semantics:

- `AddFile` adds or updates a live file entry (`kind`,`file_id`) with its level.
- `RemoveFile` deletes that entry from the live set.
- `SetCurrentVersion`, `SetLastLsn`, and `CleanShutdown` update their
  corresponding scalar state fields.

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

Current `skeindb-core` implementation status for T014:

- `.rseg` files are append-only and use `FileHeader(file_kind=RowSeg)` followed by `RecordFrame`-wrapped RV1 payloads.
- `RowSegmentWriter::append` returns a `FilePtr { file_id, offset }` pointing at the start of the emitted `RecordFrame`, so callers can chain `prev_ptr` for MVCC version histories.
- `RowGroupRef` is encoded as `group_ref_kind=0` (inline bytes) or `group_ref_kind=1` (16-byte ValueID).
- `RowSegmentReader` supports sequential full scans (`read_all`) and random-access lookup by offset (`read_at`); decode strictly rejects unknown record types/versions, unknown group ref kinds, and trailing bytes.

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

Current `skeindb-core` implementation status for T012:

- `.vseg` files are append-only and use `FileHeader(file_kind=ValSeg)` followed by framed VE1 records.
- `ValueSegmentWriter` / `ValueSegmentReader` support `codec=0` (RAW) and `codec=1` (ZSTD).
- The writer stores ZSTD only when it produces a smaller payload; otherwise it falls back to RAW.
- DELTA entries persist `DELTA1` inside `raw_bytes`; skip patches are runtime-only metadata and are rebuilt lazily rather than stored on disk.

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

Current `skeindb-core` implementation status for T013:

- `.run` files are immutable and use `FileHeader(file_kind=Run)` followed by one or more `DataBlock`s, a single `IndexBlock`, and a footer.
- `RunWriter` requires strictly increasing keys and splits data blocks by a configurable target block size.
- `RunReader` supports full scans and point lookups by binary-searching the loaded index block.
- `SimpleLsm` provides a minimal memtable + level0 implementation: puts land in a `BTreeMap`, flushes create new `run-######.run` files, and reads consult level0 runs newest-first.

### 8.1 RowDir runs (T015)

`RowDir` (`crates/skeindb-core/src/rowdir.rs`) reuses the `.run` format to
persist the `row_id → FilePtr` head-pointer directory with no new on-disk
format:

- Keys are 8-byte big-endian `row_id`, so ascending byte order matches
  ascending numeric order.
- Values are `[tag:u8][payload]`:
  - `tag = 0` — live entry; payload is the 12-byte `FilePtr` (`file_id:u32 LE`
    + `offset:u64 LE`) pointing at the head of the row's version chain in
    a `.rseg` file.
  - `tag = 1` — tombstone; no payload.
- On `load_from_run`, live entries upsert the in-memory map and tombstones
  remove the entry entirely, so newer-generation tombstones shadow older-
  generation live entries during merges.

### 8.2 MVCC visibility (T016)

Current `skeindb-core` visibility rules over `.rseg` chains:

- Readers walk `prev_ptr` from a known head `FilePtr` until they find the
  first version visible at the chosen snapshot.
- A version is visible iff `begin_ts != 0 && begin_ts <= snapshot_ts &&
  (end_ts == 0 || end_ts > snapshot_ts)`.
- `Snapshot::latest()` behaves like reading at `+INF`, so the current head
  version wins when it is committed and not superseded.
- `begin_ts == 0` means staged / not yet committed and is skipped, allowing a
  previous committed version in the chain to remain visible.
- If the first visible version is a delete marker (`flags & IS_DELETE != 0`),
  the row is considered deleted at that snapshot.

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

Current v1 WAL body types:

- `rec_type = 0x01` (`BEGIN_TXN`)
  - no body payload
- `rec_type = 0x02` (`MUTATION`)
  - `op_bytes` = Bytes (VarU length + opaque payload)
- `rec_type = 0x03` (`COMMIT_TXN`)
  - no body payload
- `rec_type = 0x04` (`ABORT_TXN`)
  - no body payload

Recovery rules for the v1 body vocabulary:

- A txn is replayable only if a valid `COMMIT_TXN` was decoded for that `txn_id`.
- `ABORT_TXN` discards any staged mutations for that `txn_id`.
- A torn or corrupt tail stops the scan at the last valid frame; recovery keeps
  all earlier valid committed txns and discards the invalid tail bytes.

---

## 10) Compaction and GC

- Compute safe_ts = oldest_active_snapshot_ts.
- Row compaction discards versions with end_ts < safe_ts.
- Value GC is mark-and-sweep driven by live row versions.

---

## 11) Prototype metadata JSON (format v1)

These JSON files are optional and may be ignored by older binaries.
Each file includes a `format_version` field; unknown versions should be ignored.

### 11.1 security_state.json

Persisted security principals for the HTTP/API bearer surface and protocol-level DB logins.

Format:

```json
{
  "format_version": 1,
  "next_api_token_id": 3,
  "api_tokens": [
    {
      "token_id": "tok_0000000000000001",
      "secret_sha256": "4a44dc15364204a80fe80e9039455cc1...",
      "role": "admin",
      "label": "ci",
      "created_at_ms": 1730000000000,
      "expires_at_ms": 0
    }
  ],
  "db_users": [
    {
      "username": "alice",
      "role": "read_write",
      "created_at_ms": 1730000000000,
      "grants": {
        "app": ["SELECT", "INSERT"]
      },
      "password_sha1": "cbfdac6008f9cab4083784cbd1874f76618d2a97",
      "password_sha256": "fcf730b6d95236ecd3c9fc2d92d7b6b2..."
    }
  ]
}
```

Compatibility notes:
- Added in v0.3 as an optional metadata file.
- Raw API token secrets are **not** stored on disk; only `secret_sha256` is persisted.
- Managed DB users persist digests (`password_sha1` for MySQL native-password verification and `password_sha256` for cleartext-password verification), never raw passwords.
- If the file is missing or has an unknown `format_version`, the server starts with no managed API tokens or DB users.

### 11.2 forensic_chain.json

Prototype forensic chain persistence used by `maintenance.audit_status`,
`maintenance.audit_verify`, and `skeindb audit-verify`.

Format:

```json
{
  "format_version": 3,
  "next_id": 4,
  "records": [
    {
      "id": 1,
      "ts_ms": 1730000000000,
      "db": "app",
      "table": "logs",
      "op": "insert",
      "pk": [{"t":"u64","v":1}],
      "change_seq": 1,
      "prev_hash": "genesis",
      "hash": "8b9d..."
    }
  ],
  "checkpoint_anchors": [
    {
      "checkpoint_id": "ckpt_1730000001000",
      "ts_ms": 1730000001000,
      "chain_len": 1,
      "chain_head_hash": "8b9d...",
      "change_seq": 1
    }
  ],
  "last_verified_ms": 1730000002000
}
```

Compatibility notes:
- Format v3 persists `last_verified_ms` so successful verification survives reopen.
- Older v1/v2 files load with `last_verified_ms = 0`.
- The prototype chain remains a stand-in for the future WAL-backed verifier.

### 11.3 merge_wasm_registry.json

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

### 11.4 views.json

Materialized-view state for `view.create/drop/refresh/evaluate/status/explain_deps`.

Format:

```json
{
  "format_version": 2,
  "views": [
    {
      "db": "app",
      "name": "city_scores",
      "query": {"body": {"select": {"projection": [], "from": []}}, "with": [], "order_by": []},
      "columns": ["city", "cnt", "total"],
      "pk_columns": ["city"],
      "deps": [
        {
          "db": "app",
          "table": "users",
          "columns": ["city", "score"],
          "projection_columns": ["city", "score"],
          "predicate_columns": [],
          "group_by_columns": ["city"]
        }
      ],
      "rows": [
        {
          "pk": [{"t": "str", "v": "Oslo"}],
          "values": [
            {"t": "str", "v": "Oslo"},
            {"t": "u64", "v": 2},
            {"t": "f64", "v": 12.0}
          ]
        }
      ],
      "source_rows": [
        {
          "pk": [{"t": "u64", "v": 1}],
          "row": {
            "id": {"t": "u64", "v": 1},
            "city": {"t": "str", "v": "Oslo"},
            "score": {"t": "u64", "v": 5}
          }
        }
      ],
      "last_refresh_ms": 1730000000000,
      "last_change_seq": 42,
      "stale": false,
      "last_refresh_mode": "incremental"
    }
  ]
}
```

Compatibility notes:
- Format v2 persists dependency-usage breakdown (`projection_columns`, `predicate_columns`, `group_by_columns`) per dependency plus grouped-view `source_rows` shadow state used by incremental maintenance.
- v0.3.15 adds the read-only `view.evaluate` oracle/benchmark and compatibility catalogs without changing this on-disk format.
- Older format v1 files still load; missing dependency-usage arrays are rebuilt from the stored query on load, and `source_rows` defaults to empty.
- If the file is missing or has an unknown `format_version`, it is ignored.

### 11.5 schema_changes.json

Prototype persisted schema-evolution proposals for `schema.propose_change`,
`schema.merge_status`, and `schema.apply_merge`.

Format:

```json
{
  "format_version": 2,
  "next_id": 3,
  "changes": [
    {
      "id": "sch_1",
      "table": {"db": "app", "table": "users"},
      "base_version": 1,
      "changes": [
        {"op": "add_column", "name": "region", "type": {"kind": "str"}, "nullable": true, "auto_increment": false},
        {"op": "add_index", "name": "region_lookup", "columns": ["region"], "unique": false}
      ],
      "message": "roll out region",
      "created_at_ms": 1730000000000,
      "status": "pending"
    }
  ]
}
```

Compatibility notes:
- Format v2 is current because persisted schema changes may now include `add_index` operations as well as `add_column`.
- Persisted schema-change `status` values now include `pending`, `applied`, and `rejected`; deterministic losers are marked `rejected` during `schema.apply_merge` without changing the file shape.
- Legacy format v1 files are still accepted on load and are rewritten to format v2 on the next persist.
- Missing files mean there are no pending schema-change proposals.
- Unknown `format_version` values are ignored by the current loader.

### 11.6 schema_flags.json

Prototype schema metadata for opt-in execution hints that should survive reopen.

Format:

```json
{
  "format_version": 1,
  "tables": [
    {
      "db": "app",
      "table": "users",
      "interned_columns": ["email", "city"]
    }
  ]
}
```

Compatibility notes:
- Added in v0.3 as an optional metadata file for Phase 15 T150.
- Missing files mean no interned-column flags are active.
- Unknown `format_version` values are ignored by the current loader.
- Column names are normalized against the live catalog on load; dropped or renamed columns are pruned from the file on the next persist.

### 11.7 wasm_catalog.json

Prototype metadata catalog for general Wasm UDF modules. Module bytes are not
embedded here; they live in the `ValueStore` and are referenced by `value_id`.

Format:

```json
{
  "format_version": 1,
  "modules": [
    {
      "module_id": "math_abs",
      "name": "math abs",
      "kind": "scalar",
      "abi": "skein.wasm.udf.v1",
      "entrypoint": "skein_scalar",
      "value_id": "0123abcd0123abcd0123abcd0123abcd",
      "size_bytes": 1234,
      "capabilities": {
        "allowed_hostcalls": ["log.debug"],
        "allowed_tables": [
          { "db": "app", "table": "users", "read": true, "write": false }
        ],
        "deterministic": true,
        "max_fuel": 1000,
        "max_memory_bytes": 65536,
        "max_output_bytes": 4096
      },
      "created_at_ms": 1730000000000
    }
  ]
}
```

Compatibility notes:
- Added in v0.3 as an optional metadata file for Phase 8 T080.
- The catalog stores only typed metadata plus `value_id`; the Wasm bytes are
  stored separately in `.vseg` data managed by `ValueStore`.
- Unknown `format_version` values are rejected by the current core loader.

### 11.8 tables/<db>/<table>.json (format v3)

Prototype row persistence for `tables/<db>/<table>.json` now supports a
ValueID-backed JSON format to reduce duplicated literal payloads in row files.

Format:

```json
{
  "format_version": 3,
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
      "schema_version": 4,
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
      "schema_version": 4,
      "deleted": false
    }
  ]
}
```

Rules:
- `"$skein_ref".id` is a 32-char hex ValueID.
- Encoders should use `"$skein_ref"` adaptively: emit refs only when repeated values
  produce net byte savings for that table snapshot.
- The first occurrence of a ValueID in a table file should include `lit` seed data.
- Later duplicates may omit `lit` and reference only `id`.
- `schema_version` records the table schema version active when that row version was written.
- Unknown `format_version` values are treated as unsupported and should fall back to legacy readers.
- v0.1/v0.2 legacy row arrays (`Vec<RowEntry>`) remain readable.
- v2 table-row payloads without `schema_version` remain readable and are normalized from `schema_versions.json` when loaded.

#### 11.8.1 Encrypted-at-rest cells (`format_version: 4`)

When a database has an active encryption profile (`ENC_RANDOM` or `ENC_MLE_DB`
with a registered active key), the engine writes table-row files with
`"format_version": 4` and replaces the value of each encryptable cell with a
self-describing `"$skein_enc"` envelope object instead of storing the plaintext
literal inline:

```json
{
  "format_version": 4,
  "rows": [
    {
      "row": {
        "id": {"t":"u64","v":1},
        "payload": {
          "$skein_enc": {
            "col": "payload",
            "kind": "str",
            "env_b64": "<base64 EncryptionEnvelope.stored_bytes()>"
          }
        }
      },
      "version": 1,
      "schema_version": 4,
      "deleted": false
    }
  ]
}
```

Rules:
- Only value-store-eligible cell kinds are encrypted (`Str`, `Json`, `Bytes`,
  `Uuid`, `Embedding`). Scalar/key cells (e.g. integer primary keys) stay
  plaintext so primary-key and secondary-index maintenance keep working.
- `"$skein_enc".env_b64` is the base64 encoding of the canonical, self-describing
  `EncryptionEnvelope` (mode tag, key id, salt/nonce, ciphertext). The plaintext
  is `serde_json::to_vec(&Lit)` of the original cell, so the literal type is
  recovered exactly on decrypt.
- Under `ENC_MLE_DB`, equal plaintext within the same encryption context yields
  an identical envelope (deterministic), which preserves equality semantics;
  under `ENC_RANDOM` each cell uses a random nonce, so value-reference dedup
  (`"$skein_ref"`) is disabled while encryption is active.
- Master keys are never persisted. If a `format_version: 4` file is loaded and no
  matching key is registered (or decryption fails), that table is marked
  **locked**: it loads with zero rows and any persist of the table is refused so
  the on-disk ciphertext is never overwritten or lost. Registering/activating the
  key transparently reloads, decrypts, and unlocks the table and rebuilds its
  indexes.
- v2/v3 row files (plaintext, optionally value-ref-backed) remain readable; v4 is
  only emitted when encryption is active for the owning database.
- Rotating the active key (`settings.encryption.rotate_key`) only flips the
  database's active key id; rows already written stay sealed under the previous
  key (still decryptable while it is registered). The
  `settings.encryption.reencrypt_table` RPC rewrites a table's backing file so
  every encrypted cell is re-sealed under the current active key, completing the
  rotation. It does not change the on-disk format (still `format_version: 4`) and
  is a no-op when the database has no active encryption mode/key.

### 11.9 tables/<db>/<table>.rseg (prototype segment container v1)

SkeinDB can also persist table rows in a compact framed container with extension `.rseg`.

Header:
- `magic[8]`: `SKNSEGR1`
- `segment_format_version` (`u32 LE`): currently `1`
- `table_format_version` (`u32 LE`): currently `3` (same row payload schema as `.json`)
- `row_count` (`u64 LE`)

Body:
- Repeated `row_count` times:
  - `payload_len` (`u32 LE`)
  - `payload` (`payload_len` bytes) as JSON-encoded `RowEntryDisk`

Behavior:
- `--storage-mode json` (or `SKEINDB_STORAGE_MODE=json`): write/read `.json`; fallback read `.rseg`.
- `--storage-mode segment` (or `SKEINDB_STORAGE_MODE=segment`): write/read `.rseg`; fallback read `.json`.
- `--storage-mode hybrid` (or `SKEINDB_STORAGE_MODE=hybrid|dual`): write both formats; read prefers `.rseg`, then `.json`.
- default mode (`serve` without `--storage-mode`): `segment`, so new deployments write/read `.rseg` and fall back to `.json`.

Compatibility notes:
- Unsupported segment header versions are ignored by fallback readers.
- v2 row payloads remain readable; missing `schema_version` fields are normalized from the per-table schema-version map on load.
- If both files are missing or unreadable, the table loads as empty.

### 11.10 tables/<db>/<table>.sidx.json (prototype secondary index cache v1)

Optional persisted cache for the engine's reusable secondary-index state.

Format:

```json
{
  "format_version": 1,
  "row_count": 2,
  "indexes": [
    {
      "built_version": 7,
      "columns": ["email"],
      "include": [],
      "keys": {
        "[{\"t\":\"str\",\"v\":\"a@example.com\"}]": [0],
        "[{\"t\":\"str\",\"v\":\"b@example.com\"}]": [1]
      }
    }
  ]
}
```

Rules:
- `row_count` must match the currently loaded table row count or the cache is ignored.
- `built_version` is the table-version snapshot the cache was built from; stale caches may be rebuilt in memory before use.
- `columns` and `include` use the same ordering semantics as the prototype secondary-index advisor.
- `keys` map the JSON-encoded composite key to row indexes inside the current table snapshot.
- Missing files or unknown `format_version` values are ignored and fall back to rebuilding from row data.

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

Prototype metadata:
- `snapshots.json` stores column snapshot metadata + row values.
- JSON includes `format_version` (current: 1) and a per-snapshot `table_version`.
- On startup, snapshots are loaded only when `table_version` matches the catalog.

Sidecar snapshot artifacts:
- `snapshots/snap-<snapshot_id>/manifest.json` stores snapshot identity, projection, table version, row count, and per-segment metadata.
- `snapshots/snap-<snapshot_id>/col-XXXX.cseg` stores one projected or primary-key column per file.
- `.cseg` files use magic `SKNCSEG1`, `format_version = 1`, a column kind byte (`pk` vs value), `snapshot_ts`, row count, db/table/column names, a null bitmap, and row-ordered non-null `Lit` payloads.

The runtime still treats `snapshots.json` as the startup cache; the sidecar snapshot directory is rebuilt best-effort on persist.

See docs/COLUMN_SNAPSHOTS.md for cseg v1.

## A.4 Index advisor telemetry (prototype)

- `advisor_patterns.json` stores aggregated query dependency patterns (format v1).
- `advisor_history.json` stores apply/dismiss actions plus lifecycle state (`status`, `progress_pct`, `result_status`, `rollback_status`, `updated_at_ms`, `error`) (format v2; legacy format v1 entries are still accepted on load).

## A.5 CDC retained change log (prototype)

- `changes.json` stores the retained change-log window used for CDC replay (format v2).
- Each record persists `seq`, `db`, `table`, `op`, optional `pk` / `before` / `after` / `query_id` / `etag`, plus `commit_ts_ms` and optional `lsn` metadata.
- Legacy format v1 envelopes and older unversioned array snapshots are still accepted on load and rewritten to the current versioned envelope on the next persist.

`cdc_subscriptions.json` stores durable consumer cursor and control state for CDC subscriptions (format v9):

```json
{
  "format_version": 9,
  "next_id": 3,
  "subs": [
    {
      "id": "sub_1",
      "acked_offset": 42,
      "paused": true,
      "options": {"format": "plain_json", "include_before": true, "include_after": true, "pk_range": {"lower_bound": {"t": "u64", "v": 2}, "upper_bound": {"t": "u64", "v": 4}}, "ops": ["update"], "columns": ["status"], "sink": {"type": "file", "path": "/var/lib/skeindb/sinks/events.ndjson"}},
      "sink_offset": 40,
      "target": {"kind": "table", "db": "app", "table": "events"}
    },
    {
      "id": "sub_2",
      "acked_offset": 42,
      "paused": false,
      "options": {"include_before": false, "include_after": false},
      "target": {"kind": "query", "query_id": "query_1", "args": []}
    }
  ]
}
```

Compatibility notes:
- Change-log format v2 adds optional retained `before` / `after` row images for CDC replay; format v1 envelopes and unversioned arrays still load.
- Format v3 adds per-subscription `options.include_before` / `options.include_after` flags used by `cdc.subscribe_table` / `cdc.subscribe_query` row-image delivery.
- Format v4 adds per-subscription `options.ops` allowlists used by `cdc.subscribe_table` / `cdc.subscribe_query` source-op filtering.
- Format v5 adds per-subscription `options.pk` exact-match primary-key filters used by `cdc.subscribe_table` / `cdc.subscribe_query` replay filtering.
- Format v6 adds per-subscription `options.columns` changed-column filters used by `cdc.subscribe_table` / `cdc.subscribe_query` replay filtering and row-image projection.
- Format v7 adds per-subscription `options.pk_range` inclusive primary-key range filters used by `cdc.subscribe_table` / `cdc.subscribe_query` replay filtering.
- Format v8 adds per-subscription `options.format` delivery encoding. Missing values default to `objects_json`; supported values are `objects_json` and `plain_json`.
- Format v9 adds the optional per-subscription `options.sink` external connector (first connector `{"type": "file", "path": ...}`) and the durable `sink_offset` cursor advanced by `cdc.sink_drain`. Missing `options.sink` defaults to no sink and missing `sink_offset` defaults to `0`.
- Format v2 adds the per-subscription `paused` flag used by `cdc.pause` / `cdc.resume` and the CDC backpressure state machine.
- Format v1/v2/v3/v4/v5/v6/v7/v8 subscription files are still accepted on load; missing `paused` defaults to `false`, missing `options` default to both row images disabled, missing `options.ops` defaults to an empty allowlist, missing `options.pk` defaults to an empty tuple filter, missing `options.pk_range` defaults to no primary-key range filter, missing `options.columns` defaults to an empty changed-column filter, missing `options.format` defaults to `objects_json`, missing `options.sink` defaults to no sink, missing `sink_offset` defaults to `0`, and the file is rewritten as v9 on the next subscription persist.
- If all subscriptions are closed, `cdc_subscriptions.json` is removed.

The CDC files are optional: missing `changes.json` means no retained replay window has been loaded, and missing `cdc_subscriptions.json` means there are no durable subscriptions to restore.

## A.6 Embedding ValueEntries

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
