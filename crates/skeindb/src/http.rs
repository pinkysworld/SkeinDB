use crate::{
    config::{ConfigError, ConfigStore, ServerConfig},
    console_db::{ConsoleDb, SqlError, SqlExecResponse, SqlResult, TableSchema},
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

const CONSOLE_HTML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/console/src/index.html"
));
const STUDIO_HTML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/console/src/studio.html"
));

#[derive(Clone)]
pub struct AppState {
    config_store: ConfigStore,
    db: Arc<RwLock<ConsoleDb>>,
    started_at_unix: u64,
    started_at: Instant,
    http_addr: SocketAddr,
    mysql_addr: SocketAddr,
}

impl AppState {
    pub fn new(
        config_store: ConfigStore,
        db: Arc<RwLock<ConsoleDb>>,
        http_addr: SocketAddr,
        mysql_addr: SocketAddr,
    ) -> Self {
        let started_at_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            config_store,
            db,
            started_at_unix,
            started_at: Instant::now(),
            http_addr,
            mysql_addr,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigResponse {
    pub config: ServerConfig,
    pub requires_restart: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SqlExecRequest {
    pub sql: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaResponse {
    pub tables: Vec<TableSchema>,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub version: String,
    pub build_os: String,
    pub build_arch: String,
    pub started_unix: u64,
    pub uptime_secs: u64,
    pub http_addr: String,
    pub mysql_addr: String,
    pub config_path: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl From<ConfigError> for ApiError {
    fn from(err: ConfigError) -> Self {
        match err {
            ConfigError::Invalid { message } => ApiError::bad_request(message),
            _ => ApiError::internal(err.to_string()),
        }
    }
}

impl From<SqlError> for ApiError {
    fn from(err: SqlError) -> Self {
        ApiError::bad_request(err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ErrorResponse {
            error: self.message,
        });
        (self.status, body).into_response()
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/console") }))
        .route("/console", get(console))
        .route("/studio", get(studio))
        .route("/api/v1/config", get(get_config).put(put_config))
        .route("/api/v1/schema", get(get_schema))
        .route("/api/v1/sql/exec", post(exec_sql))
        .route("/api/v1/table/:name", get(get_table))
        .route("/api/v1/sample/load", post(load_sample))
        .route("/api/v1/sample/sql", get(get_sample_sql))
        .route("/api/v1/status", get(get_status))
        .route("/healthz", get(healthz))
        .with_state(state)
}

pub async fn run_http_server(
    addr: SocketAddr,
    state: AppState,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "HTTP console listening");
    let app = build_router(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
}

async fn console() -> Html<&'static str> {
    Html(CONSOLE_HTML)
}

async fn studio() -> Html<&'static str> {
    Html(STUDIO_HTML)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn get_status(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    let uptime_secs = state.started_at.elapsed().as_secs();
    Ok(Json(StatusResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_os: std::env::consts::OS.to_string(),
        build_arch: std::env::consts::ARCH.to_string(),
        started_unix: state.started_at_unix,
        uptime_secs,
        http_addr: state.http_addr.to_string(),
        mysql_addr: state.mysql_addr.to_string(),
        config_path: state.config_store.path().display().to_string(),
    }))
}

async fn get_config(State(state): State<AppState>) -> Result<Json<ConfigResponse>, ApiError> {
    let config = state.config_store.get().await;
    Ok(Json(ConfigResponse {
        config,
        requires_restart: false,
    }))
}

async fn put_config(
    State(state): State<AppState>,
    Json(config): Json<ServerConfig>,
) -> Result<Json<ConfigResponse>, ApiError> {
    let previous = state.config_store.get().await;
    state.config_store.set(config.clone()).await?;
    let requires_restart = requires_restart(&previous, &config);
    Ok(Json(ConfigResponse {
        config,
        requires_restart,
    }))
}

async fn get_schema(State(state): State<AppState>) -> Result<Json<SchemaResponse>, ApiError> {
    let db = state.db.read().await;
    Ok(Json(SchemaResponse {
        tables: db.schema(),
    }))
}

#[derive(Debug, Deserialize)]
struct TableQuery {
    limit: Option<usize>,
}

async fn get_table(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<TableQuery>,
) -> Result<Json<SqlResult>, ApiError> {
    let limit = query.limit.unwrap_or(100);
    let db = state.db.read().await;
    let result = db.select_table(&name, limit)?;
    Ok(Json(result))
}

async fn exec_sql(
    State(state): State<AppState>,
    Json(request): Json<SqlExecRequest>,
) -> Result<Json<SqlExecResponse>, ApiError> {
    let mut db = state.db.write().await;
    let result = db.execute_sql(&request.sql)?;
    Ok(Json(result))
}

async fn load_sample(State(state): State<AppState>) -> Result<Json<SqlExecResponse>, ApiError> {
    let mut db = state.db.write().await;
    let result = db.load_sample()?;
    Ok(Json(result))
}

async fn get_sample_sql() -> &'static str {
    ConsoleDb::sample_sql()
}

fn requires_restart(previous: &ServerConfig, updated: &ServerConfig) -> bool {
    previous.data_dir != updated.data_dir
        || previous.mysql_port != updated.mysql_port
        || previous.http_port != updated.http_port
        || previous.allow_remote_console != updated.allow_remote_console
}
