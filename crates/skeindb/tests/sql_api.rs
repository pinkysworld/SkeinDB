use axum::{body::Body, http::Request, http::StatusCode};
use http_body_util::BodyExt;
use skeindb::{
    config::ConfigStore,
    console_db::{ConsoleDb, SqlExecResponse, SqlResult},
    http::{build_router, AppState, SchemaResponse, SqlExecRequest},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

#[tokio::test]
async fn sql_exec_and_schema_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("skeindb-config.json");
    let store = ConfigStore::load_or_default(path).await.unwrap();
    let db = Arc::new(RwLock::new(ConsoleDb::new()));
    let http_addr = "127.0.0.1:8080".parse().unwrap();
    let mysql_addr = "127.0.0.1:3306".parse().unwrap();
    let app = build_router(AppState::new(store, db, http_addr, mysql_addr));

    let sql = "CREATE TABLE users (id INT, name TEXT); \
        INSERT INTO users (id, name) VALUES (1, 'Ada');";
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sql/exec")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&SqlExecRequest {
                        sql: sql.to_string(),
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let payload: SqlExecResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(payload.results.len(), 2);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let payload: SchemaResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(payload.tables.len(), 1);
    assert_eq!(payload.tables[0].name, "users");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/table/users?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let payload: SqlResult = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(payload.rows.len(), 1);
    assert_eq!(
        payload.rows[0],
        vec![serde_json::Value::from(1), serde_json::Value::from("Ada")]
    );
}
