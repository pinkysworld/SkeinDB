use skeindb_core::encryption::{DatabaseKeyManager, EncryptionContext, EncryptionMode};
use skeindb_core::ValueKind;

fn key(fill: u8) -> [u8; 32] {
    [fill; 32]
}

#[test]
fn enc_random_roundtrip_uses_randomized_nonces() {
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(7)).unwrap();
    manager
        .set_database_mode("app", EncryptionMode::EncRandom)
        .unwrap();

    let ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);
    let plaintext = b"hello encryption";

    let first = manager.encrypt(&ctx, plaintext).unwrap();
    let second = manager.encrypt(&ctx, plaintext).unwrap();

    assert_eq!(first.mode, EncryptionMode::EncRandom);
    assert_eq!(first.scope_id, "app");
    assert_eq!(manager.decrypt(&ctx, &first).unwrap(), plaintext);
    assert_eq!(manager.decrypt(&ctx, &second).unwrap(), plaintext);
    assert_ne!(first.nonce, second.nonce);
    assert_ne!(first.stored_bytes(), second.stored_bytes());
}

#[test]
fn enc_mle_db_roundtrip_is_deterministic_within_database_scope() {
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(11)).unwrap();
    manager
        .set_database_mode("app", EncryptionMode::EncMleDb)
        .unwrap();

    let ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);
    let plaintext = b"same plaintext same scope";

    let first = manager.encrypt(&ctx, plaintext).unwrap();
    let second = manager.encrypt(&ctx, plaintext).unwrap();

    assert_eq!(first.mode, EncryptionMode::EncMleDb);
    assert_eq!(first, second);
    assert_eq!(manager.decrypt(&ctx, &first).unwrap(), plaintext);
}

#[test]
fn enc_mle_db_binds_context_and_database_key_scope() {
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(17)).unwrap();
    manager
        .register_database_key("analytics", "k1", key(29))
        .unwrap();
    manager
        .set_database_mode("app", EncryptionMode::EncMleDb)
        .unwrap();
    manager
        .set_database_mode("analytics", EncryptionMode::EncMleDb)
        .unwrap();

    let app_ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);
    let analytics_ctx =
        EncryptionContext::new("analytics", "events", "payload", ValueKind::Cell, 1);
    let wrong_ctx = EncryptionContext::new("app", "events", "other_col", ValueKind::Cell, 1);
    let plaintext = b"scope sensitive";

    let app_env = manager.encrypt(&app_ctx, plaintext).unwrap();
    let analytics_env = manager.encrypt(&analytics_ctx, plaintext).unwrap();

    assert_ne!(app_env.stored_bytes(), analytics_env.stored_bytes());
    assert!(manager.decrypt(&wrong_ctx, &app_env).is_err());
}

#[test]
fn envelope_stored_bytes_roundtrip_via_from_stored_bytes() {
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(31)).unwrap();
    manager
        .set_database_mode("app", EncryptionMode::EncRandom)
        .unwrap();
    let ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);
    let env = manager.encrypt(&ctx, b"reparse me").unwrap();
    let bytes = env.stored_bytes();
    let back = skeindb_core::encryption::EncryptionEnvelope::from_stored_bytes(&bytes).unwrap();
    assert_eq!(env, back);
    assert_eq!(manager.decrypt(&ctx, &back).unwrap(), b"reparse me");

    let mut bad = bytes.clone();
    bad.push(0xff);
    assert!(skeindb_core::encryption::EncryptionEnvelope::from_stored_bytes(&bad).is_err());
}

#[test]
fn encrypted_value_store_roundtrip_under_three_modes() {
    use skeindb_core::encrypted_valuestore::EncryptedValueStore;
    use skeindb_core::valuestore::{ValueStore, ValueStoreConfig};

    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(41)).unwrap();
    let ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);

    for mode in [
        EncryptionMode::Off,
        EncryptionMode::EncRandom,
        EncryptionMode::EncMleDb,
    ] {
        manager.set_database_mode("app", mode).unwrap();
        let mut wrap = EncryptedValueStore::new(&mut store, &manager);
        let plaintext = format!("hello-mode-{}", mode.as_str()).into_bytes();
        let rec = wrap.put_encrypted(&ctx, &plaintext).unwrap();
        assert_eq!(rec.mode, mode);
        let plain = wrap.get_decrypted(&ctx, &rec.value_id).unwrap();
        assert_eq!(plain, plaintext);
    }
}

#[test]
fn rotate_active_key_then_reencrypt_envelope_progress_counters() {
    use skeindb_core::encryption::ReencryptionProgress;
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(53)).unwrap();
    manager.register_database_key("app", "k2", key(59)).unwrap();
    manager
        .set_database_mode("app", EncryptionMode::EncRandom)
        .unwrap();

    let ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);
    let env_old = manager.encrypt(&ctx, b"rotate me").unwrap();
    assert_eq!(env_old.key_id.as_deref(), Some("k1"));

    let plan = manager.rotate_active_key("app", "k2").unwrap();
    assert_eq!(plan.previous_key_id.as_deref(), Some("k1"));
    assert_eq!(plan.new_key_id, "k2");
    assert_eq!(plan.mode, EncryptionMode::EncRandom);

    let mut progress = ReencryptionProgress::new("app");
    let new_env = manager
        .reencrypt_envelope(&ctx, &env_old, &mut progress)
        .unwrap()
        .expect("envelope should be rewritten under k2");
    assert_eq!(new_env.key_id.as_deref(), Some("k2"));
    assert_eq!(progress.envelopes_inspected, 1);
    assert_eq!(progress.envelopes_rewritten, 1);
    assert_eq!(progress.envelopes_skipped_off, 0);
    assert_eq!(progress.envelopes_skipped_current, 0);
    assert_eq!(progress.previous_key_id.as_deref(), Some("k1"));
    assert_eq!(progress.new_key_id.as_deref(), Some("k2"));

    // Old key must still decrypt the historical envelope.
    let plain_old = manager.decrypt(&ctx, &env_old).unwrap();
    let plain_new = manager.decrypt(&ctx, &new_env).unwrap();
    assert_eq!(plain_old, plain_new);

    // Re-running on the freshly rewritten envelope is a no-op.
    let mut progress2 = ReencryptionProgress::new("app");
    let again = manager
        .reencrypt_envelope(&ctx, &new_env, &mut progress2)
        .unwrap();
    assert!(again.is_none());
    assert_eq!(progress2.envelopes_inspected, 1);
    assert_eq!(progress2.envelopes_rewritten, 0);
    assert_eq!(progress2.envelopes_skipped_current, 1);
}

#[test]
fn encrypted_value_store_reencrypt_value_writes_new_envelope() {
    use skeindb_core::encrypted_valuestore::EncryptedValueStore;
    use skeindb_core::encryption::ReencryptionProgress;
    use skeindb_core::valuestore::{ValueStore, ValueStoreConfig};

    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(71)).unwrap();
    manager.register_database_key("app", "k2", key(73)).unwrap();
    manager
        .set_database_mode("app", EncryptionMode::EncRandom)
        .unwrap();
    let ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);

    let value_id = {
        let mut wrap = EncryptedValueStore::new(&mut store, &manager);
        wrap.put_encrypted(&ctx, b"to be rotated").unwrap().value_id
    };

    let _plan = manager.rotate_active_key("app", "k2").unwrap();

    let new_id = {
        let mut progress = ReencryptionProgress::new("app");
        let mut wrap = EncryptedValueStore::new(&mut store, &manager);
        let new_id = wrap
            .reencrypt_value(&ctx, &value_id, &mut progress)
            .unwrap()
            .expect("rotation should yield a new ValueId");
        assert_eq!(progress.envelopes_rewritten, 1);
        new_id
    };
    assert_ne!(new_id, value_id);

    // Both old and new ValueIds remain readable under their respective keys.
    let mut wrap = EncryptedValueStore::new(&mut store, &manager);
    assert_eq!(
        wrap.get_decrypted(&ctx, &value_id).unwrap(),
        b"to be rotated"
    );
    assert_eq!(wrap.get_decrypted(&ctx, &new_id).unwrap(), b"to be rotated");
    let new_env = wrap.read_envelope(&new_id).unwrap();
    assert_eq!(new_env.key_id.as_deref(), Some("k2"));
}

#[test]
fn encrypted_value_store_reencrypt_values_sweeps_batch_under_active_key() {
    use skeindb_core::encrypted_valuestore::EncryptedValueStore;
    use skeindb_core::encryption::ReencryptionProgress;
    use skeindb_core::valuestore::{ValueStore, ValueStoreConfig};

    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut manager = DatabaseKeyManager::new();
    manager.register_database_key("app", "k1", key(81)).unwrap();
    manager.register_database_key("app", "k2", key(83)).unwrap();
    manager
        .set_database_mode("app", EncryptionMode::EncRandom)
        .unwrap();
    let ctx = EncryptionContext::new("app", "events", "payload", ValueKind::Cell, 1);

    let payloads: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
    let value_ids = {
        let mut wrap = EncryptedValueStore::new(&mut store, &manager);
        payloads
            .iter()
            .map(|p| wrap.put_encrypted(&ctx, p).unwrap().value_id)
            .collect::<Vec<_>>()
    };

    manager.rotate_active_key("app", "k2").unwrap();

    let mut progress = ReencryptionProgress::new("app");
    let mapping = {
        let mut wrap = EncryptedValueStore::new(&mut store, &manager);
        wrap.reencrypt_values(&ctx, &value_ids, &mut progress)
            .unwrap()
    };

    // Every envelope was rewritten under the new key.
    assert_eq!(mapping.len(), 3);
    assert_eq!(progress.envelopes_inspected, 3);
    assert_eq!(progress.envelopes_rewritten, 3);
    assert_eq!(progress.envelopes_skipped_current, 0);

    let mut wrap = EncryptedValueStore::new(&mut store, &manager);
    for ((old_id, new_id), expected) in mapping.iter().zip(payloads.iter()) {
        assert_ne!(old_id, new_id);
        // Old and new ValueIds both decrypt to the original plaintext.
        assert_eq!(&wrap.get_decrypted(&ctx, old_id).unwrap(), expected);
        assert_eq!(&wrap.get_decrypted(&ctx, new_id).unwrap(), expected);
        assert_eq!(
            wrap.read_envelope(new_id).unwrap().key_id.as_deref(),
            Some("k2")
        );
    }

    // A second sweep over the rewritten ids is a no-op (already current).
    let new_ids = mapping.iter().map(|(_, n)| *n).collect::<Vec<_>>();
    let mut progress2 = ReencryptionProgress::new("app");
    let again = wrap
        .reencrypt_values(&ctx, &new_ids, &mut progress2)
        .unwrap();
    assert!(again.is_empty());
    assert_eq!(progress2.envelopes_inspected, 3);
    assert_eq!(progress2.envelopes_rewritten, 0);
    assert_eq!(progress2.envelopes_skipped_current, 3);
}
