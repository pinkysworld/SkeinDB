use skeindb_core::valuestore::{
    BloomFilter, DeltaPolicy, ModelRefreshPolicy, ValueEntry, ValueId, ValueSegmentReader,
    ValueSegmentWriter, ValueStore, ValueStoreConfig,
};
use skeindb_core::{value_id, FileHeader, FileKind, ValueKind};

fn temp_path(label: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!(
        "skeindb_core_vseg_{}_{}_{}",
        label,
        std::process::id(),
        ts
    ));
    path
}

fn next_u64(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    *seed
}

fn make_bytes(seed: &mut u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push((next_u64(seed) >> 32) as u8);
    }
    out
}

fn value_id_from_u64(value: u64) -> ValueId {
    let mut id = [0u8; 16];
    id[0..8].copy_from_slice(&value.to_be_bytes());
    id
}

#[test]
fn learned_index_lookup_hits() {
    let config = ValueStoreConfig {
        enable_learned_index: true,
        segment_size: 32,
        refresh_policy: ModelRefreshPolicy {
            min_samples: 64,
            max_inserts: 10_000,
            max_shift_score: 0.5,
        },
        delta_policy: DeltaPolicy::default(),
    };
    let mut store = ValueStore::new(config);

    let mut seed = 1u64;
    let mut ids = Vec::new();
    let mut values = Vec::new();
    for _ in 0..256 {
        let bytes = make_bytes(&mut seed, 24);
        let id = store.put(ValueKind::Cell, bytes.clone());
        ids.push(id);
        values.push(bytes);
    }

    store.refresh_learned_index();

    for (id, bytes) in ids.iter().zip(values.iter()) {
        let entry = store.get(id).expect("value present");
        assert_eq!(&entry.bytes, bytes);
    }

    let stats = store.stats();
    assert!(stats.learned_hits > 0);
}

#[test]
fn learned_index_report_describes_offline_model_and_fallback() {
    let config = ValueStoreConfig {
        enable_learned_index: true,
        segment_size: 16,
        refresh_policy: ModelRefreshPolicy {
            min_samples: 16,
            max_inserts: 10_000,
            max_shift_score: 0.5,
        },
        delta_policy: DeltaPolicy::default(),
    };
    let mut store = ValueStore::new(config);

    for i in 0..64u64 {
        let id = value_id_from_u64(i);
        store.put_with_id(ValueKind::Cell, id, format!("value-{i}").into_bytes());
    }

    store.refresh_learned_index();
    let report = store.learned_index_report(2);

    assert!(report.enabled);
    assert!(report.built);
    assert_eq!(report.total_keys, 64);
    assert_eq!(report.segment_count, 4);
    assert_eq!(report.configured_segment_size, 16);
    assert_eq!(report.max_error, 0);
    assert_eq!(report.max_search_window, 3);
    assert!(report.approx_model_bytes > 0);
    assert_eq!(report.fallback_entries, 64);
    assert!(report.approx_fallback_bytes > 0);
    assert_eq!(report.segments.len(), 2);
    assert_eq!(report.segments[0].start_position, 0);
    assert_eq!(report.segments[0].end_position, 15);
    assert_eq!(report.segments[0].start_key_prefix_hex, "0000000000000000");
    assert_eq!(report.segments[0].end_key_prefix_hex, "000000000000000f");
    assert_eq!(report.segments[0].search_window, 3);
}

#[test]
fn lookup_distribution_exports_histogram_and_top_buckets() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let hot_id: ValueId = [0x2a; 16];
    let cold_id: ValueId = [0x7f; 16];

    store.put_with_id(ValueKind::Cell, hot_id, b"hot".to_vec());
    store.put_with_id(ValueKind::Cell, cold_id, b"cold".to_vec());

    for _ in 0..3 {
        assert!(store.get(&hot_id).is_some());
    }
    assert!(store.get(&cold_id).is_some());

    let report = store.lookup_distribution(2);
    assert_eq!(report.total_lookups, 4);
    assert_eq!(report.non_empty_buckets, 2);
    assert_eq!(report.buckets.len(), 256);
    assert_eq!(report.buckets[0x2a], 3);
    assert_eq!(report.buckets[0x7f], 1);
    assert_eq!(report.top_buckets.len(), 2);
    assert_eq!(report.top_buckets[0].prefix, 0x2a);
    assert_eq!(report.top_buckets[0].prefix_hex, "2a");
    assert_eq!(report.top_buckets[0].count, 3);
    assert!(report.top_buckets[0].share > report.top_buckets[1].share);
}

#[test]
fn put_with_id_stores_custom_id() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let id = [7u8; 16];
    let bytes = b"vector-blob".to_vec();

    let stored = store.put_with_id(ValueKind::Embedding, id, bytes.clone());
    assert_eq!(stored, id);
    let entry = store.get(&id).expect("entry present");
    assert_eq!(entry.bytes, bytes);
    assert_eq!(entry.kind, ValueKind::Embedding);

    let stored_again = store.put_with_id(ValueKind::Embedding, id, b"other".to_vec());
    assert_eq!(stored_again, id);
    let entry = store.get(&id).expect("entry still present");
    assert_eq!(entry.bytes, bytes);
}

#[test]
fn learned_index_falls_back_for_new_keys() {
    let config = ValueStoreConfig {
        enable_learned_index: true,
        segment_size: 32,
        refresh_policy: ModelRefreshPolicy {
            min_samples: 64,
            max_inserts: 1_000_000,
            max_shift_score: 0.9,
        },
        delta_policy: DeltaPolicy::default(),
    };
    let mut store = ValueStore::new(config);

    let mut seed = 7u64;
    for _ in 0..128 {
        let bytes = make_bytes(&mut seed, 16);
        store.put(ValueKind::Cell, bytes);
    }
    store.refresh_learned_index();

    let new_bytes = make_bytes(&mut seed, 16);
    let new_id = store.put(ValueKind::Cell, new_bytes.clone());

    let (entry, trace) = store.get_with_trace(&new_id);
    assert!(entry.is_some());
    assert!(trace.used_fallback);
}

#[test]
fn distribution_shift_triggers_refresh() {
    let config = ValueStoreConfig {
        enable_learned_index: true,
        segment_size: 64,
        refresh_policy: ModelRefreshPolicy {
            min_samples: 64,
            max_inserts: 10_000,
            max_shift_score: 0.05,
        },
        delta_policy: DeltaPolicy::default(),
    };
    let mut store = ValueStore::new(config);

    let mut seed = 42u64;
    let mut hot_ids = Vec::new();
    let mut all_ids = Vec::new();
    let mut attempts = 0u64;
    while hot_ids.len() < 64 && attempts < 200_000 {
        attempts += 1;
        let bytes = make_bytes(&mut seed, 20);
        let id = store.put(ValueKind::Cell, bytes);
        all_ids.push(id);
        if id[0] < 16 {
            hot_ids.push(id);
        }
    }

    assert!(!hot_ids.is_empty(), "failed to find hot bucket ids");
    for id in all_ids.iter() {
        let _ = store.get(id);
    }
    store.refresh_learned_index();

    for i in 0..20000 {
        let id = &hot_ids[i % hot_ids.len()];
        let _ = store.get(id);
    }

    assert!(store.should_refresh());
}

#[test]
fn benchmark_reports_quantiles() {
    let config = ValueStoreConfig {
        enable_learned_index: true,
        segment_size: 32,
        refresh_policy: ModelRefreshPolicy {
            min_samples: 128,
            max_inserts: 10_000,
            max_shift_score: 0.5,
        },
        delta_policy: DeltaPolicy::default(),
    };
    let mut store = ValueStore::new(config);
    let mut seed = 9u64;
    let mut ids = Vec::new();
    for _ in 0..512 {
        let bytes = make_bytes(&mut seed, 18);
        let id = store.put(ValueKind::Cell, bytes);
        ids.push(id);
    }

    store.refresh_learned_index();
    let bench = store.benchmark(&ids);

    assert!(bench.lookups >= ids.len());
    assert!(bench.p50_probes <= bench.p99_probes);
    assert!(bench.p99_probes <= bench.p999_probes);
    assert!(bench.memory_bytes > 0);
}

#[test]
fn delta_patch_roundtrip() {
    let config = ValueStoreConfig {
        delta_policy: DeltaPolicy {
            enabled: true,
            min_bytes: 1,
            max_chain: 8,
            min_savings_ratio: 0.9,
            snapshot_interval: 0,
            max_skip: 0,
        },
        ..ValueStoreConfig::default()
    };
    let mut store = ValueStore::new(config);

    let base = vec![b'a'; 256];
    let base_id = store.put(ValueKind::Cell, base.clone());
    let mut updated = base.clone();
    updated[128] = b'b';

    let new_id = store.put_with_delta(ValueKind::Cell, updated.clone(), Some(base_id));
    let entry = store.get(&new_id).expect("entry");
    assert_eq!(entry.kind, ValueKind::Delta);
    assert!(entry.delta.is_some());

    let (materialized, trace) = store.materialize_with_trace(&new_id).expect("materialize");
    assert_eq!(materialized, updated);
    assert!(trace.steps >= 1);
}

#[test]
fn delta_snapshot_interval_enforces_raw() {
    let config = ValueStoreConfig {
        delta_policy: DeltaPolicy {
            enabled: true,
            min_bytes: 1,
            max_chain: 32,
            min_savings_ratio: 0.9,
            snapshot_interval: 3,
            max_skip: 0,
        },
        ..ValueStoreConfig::default()
    };
    let mut store = ValueStore::new(config);

    let base = b"value-0000".to_vec();
    let mut prev = store.put(ValueKind::Cell, base);
    let mut ids = Vec::new();

    for i in 1..=6 {
        let bytes = format!("value-{i:04}").into_bytes();
        let id = store.put_with_delta(ValueKind::Cell, bytes, Some(prev));
        ids.push(id);
        prev = id;
    }

    assert_eq!(store.get(&ids[2]).unwrap().kind, ValueKind::Cell);
    assert_eq!(store.get(&ids[5]).unwrap().kind, ValueKind::Cell);
}

#[test]
fn skip_patches_reduce_steps() {
    let config = ValueStoreConfig {
        delta_policy: DeltaPolicy {
            enabled: true,
            min_bytes: 1,
            max_chain: 32,
            min_savings_ratio: 0.9,
            snapshot_interval: 0,
            max_skip: 4,
        },
        ..ValueStoreConfig::default()
    };
    let mut store = ValueStore::new(config);

    let base = b"base-0000".to_vec();
    let mut prev = store.put(ValueKind::Cell, base);
    for i in 1..=8 {
        let bytes = format!("base-{i:04}").into_bytes();
        prev = store.put_with_delta(ValueKind::Cell, bytes, Some(prev));
    }

    let depth = store.delta_chain_depth(&prev).unwrap_or(0);
    let (_bytes, trace) = store.materialize_with_trace(&prev).expect("materialize");
    assert!(trace.used_skip);
    assert!(trace.steps < depth);
}

#[test]
fn delta_compaction_rewrites_deep_chains() {
    let mut store = ValueStore::new(ValueStoreConfig {
        delta_policy: DeltaPolicy {
            enabled: true,
            min_bytes: 1,
            max_chain: 16,
            min_savings_ratio: 0.9,
            snapshot_interval: 0,
            max_skip: 0,
        },
        ..ValueStoreConfig::default()
    });

    let base = b"comp-0000".to_vec();
    let mut prev = store.put(ValueKind::Cell, base);
    for i in 1..=5 {
        let bytes = format!("comp-{i:04}").into_bytes();
        prev = store.put_with_delta(ValueKind::Cell, bytes, Some(prev));
    }

    store.set_delta_policy(DeltaPolicy {
        enabled: true,
        min_bytes: 1,
        max_chain: 2,
        min_savings_ratio: 0.9,
        snapshot_interval: 0,
        max_skip: 0,
    });

    let report = store.compact_deltas();
    assert!(report.rewritten > 0);
    assert!(report.energy.io_bytes_read > 0);
    assert!(report.energy.cpu_units > 0);
    let entry = store.get(&prev).unwrap();
    assert_eq!(entry.kind, ValueKind::Cell);
}

// ── T165: Bloom filter tests ────────────────────────────────────────────────

#[test]
fn bloom_filter_basic_membership() {
    let mut bf = BloomFilter::new(256);
    let id1: ValueId = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let id2: ValueId = [0; 16];
    let missing: ValueId = [255; 16];

    bf.insert(&id1);
    bf.insert(&id2);

    assert!(bf.maybe_contains(&id1));
    assert!(bf.maybe_contains(&id2));
    // Probabilistic: might return true, but extremely unlikely for a fresh filter
    // We just verify the API works correctly without panicking.
    assert_eq!(bf.count(), 2);
    assert!(bf.size_bytes() > 0);
    assert!(bf.estimated_fpr() < 1.0);
    // Verify missing ID is very likely absent for low-fill filter
    assert!(!bf.maybe_contains(&missing));
}

#[test]
fn bloom_filter_integrated_with_valuestore() {
    let store_cfg = ValueStoreConfig {
        enable_learned_index: false,
        ..Default::default()
    };
    let mut store = ValueStore::new(store_cfg);

    let data1 = b"bloom_test_value_1".to_vec();
    let data2 = b"bloom_test_value_2".to_vec();
    let id1 = store.put(ValueKind::Cell, data1);
    let id2 = store.put(ValueKind::Cell, data2);

    // Both should be in Bloom and in HashMap
    assert!(store.bloom_maybe_contains(&id1));
    assert!(store.bloom_maybe_contains(&id2));
    assert!(store.contains(id1));
    assert!(store.contains(id2));

    // An ID we never inserted should (very likely) not be in Bloom
    let fake_id: ValueId = [0xAA; 16];
    assert!(!store.contains(fake_id));
    // Bloom might say true (false positive) but for a nearly-empty filter it should be false
    // We don't assert this as it's probabilistic

    let (count, size, fpr) = store.bloom_stats();
    assert_eq!(count, 2);
    assert!(size > 0);
    assert!(fpr < 0.01);
}

#[test]
fn value_segment_roundtrip_preserves_ids_and_delta_materialization() {
    let path = temp_path("roundtrip");
    let config = ValueStoreConfig {
        delta_policy: DeltaPolicy {
            enabled: true,
            min_bytes: 1,
            max_chain: 8,
            min_savings_ratio: 0.9,
            snapshot_interval: 0,
            max_skip: 4,
        },
        ..ValueStoreConfig::default()
    };
    let mut store = ValueStore::new(config.clone());

    let base = b"hello-delta-base".to_vec();
    let base_id = store.put(ValueKind::Cell, base.clone());
    let mut updated = base.clone();
    updated[6] = b'X';
    let delta_id = store.put_with_delta(ValueKind::Cell, updated.clone(), Some(base_id));

    let embedding_id = [9u8; 16];
    store.put_with_id(ValueKind::Embedding, embedding_id, b"vec-blob".to_vec());

    store.write_segment_file(&path).expect("write vseg");

    let mut loaded = ValueStore::load_segment_file(&path, config).expect("load vseg");
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded.get(&base_id).expect("base").bytes, base);
    assert_eq!(
        loaded.get(&embedding_id).expect("embedding").bytes,
        b"vec-blob"
    );
    assert_eq!(loaded.get(&delta_id).expect("delta").kind, ValueKind::Delta);
    assert_eq!(loaded.materialize(&delta_id).expect("materialize"), updated);

    let _ = std::fs::remove_file(path);
}

#[test]
fn value_segment_writer_reopen_appends_records() {
    let path = temp_path("append_reopen");
    let id1 = value_id(b"one");
    let id2 = value_id(b"two");

    {
        let mut writer = ValueSegmentWriter::create(&path).expect("create writer");
        writer
            .append(
                id1,
                &ValueEntry {
                    kind: ValueKind::Cell,
                    bytes: b"one".to_vec(),
                    delta: None,
                },
            )
            .expect("append one");
        writer.sync().expect("sync one");
    }
    {
        let mut writer = ValueSegmentWriter::open(&path).expect("reopen writer");
        writer
            .append(
                id2,
                &ValueEntry {
                    kind: ValueKind::Cell,
                    bytes: b"two".to_vec(),
                    delta: None,
                },
            )
            .expect("append two");
        writer.sync().expect("sync two");
    }

    let entries = ValueSegmentReader::open(&path)
        .expect("open reader")
        .read_all()
        .expect("read all");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, id1);
    assert_eq!(entries[0].entry.bytes, b"one");
    assert_eq!(entries[1].id, id2);
    assert_eq!(entries[1].entry.bytes, b"two");

    let _ = std::fs::remove_file(path);
}

#[test]
fn value_segment_reader_rejects_non_valseg_header() {
    let path = temp_path("bad_header");
    std::fs::write(
        &path,
        FileHeader::new(FileKind::Manifest, 0, 1_700_000_000).encode(),
    )
    .expect("write manifest header");

    assert!(ValueSegmentReader::open(&path).is_err());

    let _ = std::fs::remove_file(path);
}
