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
