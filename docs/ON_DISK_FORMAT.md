# SkeinDB On-Disk Format v0.1

Status: Draft v0.1 (implementable)
Last updated: 2026-01-17

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
  val_kind     u8  (1=CELL, 2=GROUP, 3=BLOB_CHUNK, 4=BLOB_MANIFEST)
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
