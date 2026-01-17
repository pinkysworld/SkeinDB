use axum::{body::Body, http::Request, http::StatusCode};
use http_body_util::BodyExt;
use skeindb::{
    config::ConfigStore,
    console_db::ConsoleDb,
    http::{build_router, AppState, ConfigResponse},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

#[tokio::test]
async fn http_config_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("skeindb-config.json");
    let store = ConfigStore::load_or_default(path).await.unwrap();
    let db = Arc::new(RwLock::new(ConsoleDb::new()));
    let http_addr = "127.0.0.1:8080".parse().unwrap();
    let mysql_addr = "127.0.0.1:3306".parse().unwrap();
    let app = build_router(AppState::new(store.clone(), db, http_addr, mysql_addr));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let mut payload: ConfigResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(payload.config.mysql_port, 3306);

    payload.config.max_connections = 512;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload.config).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let payload: ConfigResponse = serde_json::from_slice(&bytes).unwrap();
    assert!(!payload.requires_restart);

    let mut updated = payload.config.clone();
    updated.mysql_port = 3307;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&updated).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let payload: ConfigResponse = serde_json::from_slice(&bytes).unwrap();
    assert!(payload.requires_restart);

    let stored = store.get().await;
    assert_eq!(stored.mysql_port, 3307);
}

#[tokio::test]
async fn http_pages_served() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("skeindb-config.json");
    let store = ConfigStore::load_or_default(path).await.unwrap();
    let db = Arc::new(RwLock::new(ConsoleDb::new()));
    let http_addr = "127.0.0.1:8080".parse().unwrap();
    let mysql_addr = "127.0.0.1:3306".parse().unwrap();
    let app = build_router(AppState::new(store, db, http_addr, mysql_addr));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/console")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&bytes);
    assert!(body.contains("SkeinDB Console"));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/studio")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&bytes);
    assert!(body.contains("SkeinDB SQL Studio"));
}
