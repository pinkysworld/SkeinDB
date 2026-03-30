use skeindb_core::valuestore::{
    BloomFilter, DeltaPolicy, ModelRefreshPolicy, ValueId, ValueStore, ValueStoreConfig,
};
use skeindb_core::ValueKind;

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
