//! PostgreSQL v3 wire protocol primitives.
//!
//! Message format (frontend & backend):
//!   [1-byte tag] [4-byte BE length (includes self)] [payload]
//!
//! Exception: the startup message has no tag byte — it starts with
//! [4-byte length] [protocol version 196608 (3.0)] [key=val\0 pairs] [\0].

use std::collections::HashMap;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

/// Protocol version 3.0 encoded as i32 (major << 16 | minor).
pub const PG_PROTOCOL_V3: i32 = 196608; // 3 << 16

/// SSLRequest code (used when client wants TLS negotiation before startup).
pub const PG_SSL_REQUEST_CODE: i32 = 80877103;

/// CancelRequest code.
pub const PG_CANCEL_REQUEST_CODE: i32 = 80877102;

/// Server version string returned in ParameterStatus and `SELECT version()`.
pub const PG_SERVER_VERSION: &str = "16.0 (SkeinDB compatibility)";

/// Backend message tags.
pub mod backend {
    pub const AUTHENTICATION: u8 = b'R';
    pub const PARAMETER_STATUS: u8 = b'S';
    pub const BACKEND_KEY_DATA: u8 = b'K';
    pub const READY_FOR_QUERY: u8 = b'Z';
    pub const ROW_DESCRIPTION: u8 = b'T';
    pub const DATA_ROW: u8 = b'D';
    pub const COMMAND_COMPLETE: u8 = b'C';
    pub const ERROR_RESPONSE: u8 = b'E';
    pub const NOTICE_RESPONSE: u8 = b'N';
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
    pub const PARSE_COMPLETE: u8 = b'1';
    pub const BIND_COMPLETE: u8 = b'2';
    pub const CLOSE_COMPLETE: u8 = b'3';
    pub const NO_DATA: u8 = b'n';
    pub const PARAMETER_DESCRIPTION: u8 = b't';
}

/// Frontend message tags.
pub mod frontend {
    pub const QUERY: u8 = b'Q';
    pub const PARSE: u8 = b'P';
    pub const BIND: u8 = b'B';
    pub const DESCRIBE: u8 = b'D';
    pub const EXECUTE: u8 = b'E';
    pub const SYNC: u8 = b'S';
    pub const CLOSE: u8 = b'C';
    pub const FLUSH: u8 = b'H';
    pub const TERMINATE: u8 = b'X';
    pub const PASSWORD_MESSAGE: u8 = b'p';
}

/// Transaction status indicators for ReadyForQuery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    /// Idle (not in a transaction block).
    Idle,
    /// Inside a transaction block.
    InTransaction,
    /// Failed transaction block (commands will be rejected until ROLLBACK).
    Failed,
}

impl TxStatus {
    pub fn as_byte(self) -> u8 {
        match self {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        }
    }
}

// ---------------------------------------------------------------------------
// Authentication types
// ---------------------------------------------------------------------------

/// Authentication sub-types (first 4 bytes of AuthenticationXxx payload).
pub mod auth {
    pub const OK: i32 = 0;
    pub const CLEARTEXT_PASSWORD: i32 = 3;
    pub const SASL: i32 = 10;
    pub const SASL_CONTINUE: i32 = 11;
    pub const SASL_FINAL: i32 = 12;
}

// ---------------------------------------------------------------------------
// Startup message
// ---------------------------------------------------------------------------

/// Parsed content of a StartupMessage (protocol v3).
#[derive(Debug, Clone)]
pub struct StartupMessage {
    pub protocol_version: i32,
    pub params: HashMap<String, String>,
}

impl StartupMessage {
    /// Extract `user` parameter (mandatory per PG spec).
    pub fn user(&self) -> Option<&str> {
        self.params.get("user").map(|s| s.as_str())
    }

    /// Extract `database` parameter (defaults to user name if absent).
    pub fn database(&self) -> Option<&str> {
        self.params.get("database").map(|s| s.as_str())
    }
}

/// Read the initial startup message from the client.
///
/// Returns `Ok(None)` when the client sends an SSLRequest (the caller should
/// respond with `b'N'` and then call this again for the real startup message).
pub async fn read_startup_message(
    stream: &mut TcpStream,
) -> anyhow::Result<Option<StartupMessage>> {
    // First 4 bytes: message length (includes self).
    let len = stream.read_i32().await? as usize;
    if len < 8 || len > 10240 {
        anyhow::bail!("startup message length out of range: {len}");
    }
    let payload_len = len - 4; // we already read the 4-byte length
    let mut buf = vec![0u8; payload_len];
    stream.read_exact(&mut buf).await?;

    let version = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

    if version == PG_SSL_REQUEST_CODE {
        return Ok(None); // SSLRequest — caller should reply 'N' and retry
    }

    if version == PG_CANCEL_REQUEST_CODE {
        anyhow::bail!("CancelRequest not supported");
    }

    // Parse key=value pairs from buf[4..] — pairs of C-strings, terminated by \0.
    let mut params = HashMap::new();
    let mut pos = 4;
    loop {
        if pos >= buf.len() {
            break;
        }
        let key = read_cstring(&buf, &mut pos);
        if key.is_empty() {
            break;
        }
        let value = read_cstring(&buf, &mut pos);
        params.insert(key, value);
    }

    Ok(Some(StartupMessage {
        protocol_version: version,
        params,
    }))
}

// ---------------------------------------------------------------------------
// Frontend (client→server) message reading
// ---------------------------------------------------------------------------

/// A parsed frontend message with its tag byte and payload.
#[derive(Debug)]
pub struct FrontendMessage {
    pub tag: u8,
    pub payload: Vec<u8>,
}

/// Read a single frontend message (tag + length-prefixed body).
pub async fn read_message(stream: &mut TcpStream) -> anyhow::Result<FrontendMessage> {
    let tag = stream.read_u8().await?;
    let len = stream.read_i32().await? as usize;
    if len < 4 {
        anyhow::bail!("message length too short: {len}");
    }
    let payload_len = len - 4;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok(FrontendMessage { tag, payload })
}

/// Parse a simple Query message payload → SQL string.
pub fn parse_query(payload: &[u8]) -> String {
    // Query payload is a single C-string.
    let end = payload
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(payload.len());
    String::from_utf8_lossy(&payload[..end]).to_string()
}

// ---------------------------------------------------------------------------
// Backend (server→client) message builders
// ---------------------------------------------------------------------------

/// Write a tagged backend message.
pub async fn write_message(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> anyhow::Result<()> {
    let len = (payload.len() + 4) as i32;
    stream.write_u8(tag).await?;
    stream.write_i32(len).await?;
    if !payload.is_empty() {
        stream.write_all(payload).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Build and write an AuthenticationOk message.
pub async fn write_auth_ok(stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(4);
    buf.extend_from_slice(&(auth::OK as i32).to_be_bytes());
    write_message(stream, backend::AUTHENTICATION, &buf).await
}

/// Build and write an AuthenticationCleartextPassword request.
pub async fn write_auth_cleartext_password(stream: &mut TcpStream) -> anyhow::Result<()> {
    let buf = (auth::CLEARTEXT_PASSWORD as i32).to_be_bytes();
    write_message(stream, backend::AUTHENTICATION, &buf).await
}

/// Build and write a ParameterStatus message.
pub async fn write_parameter_status(
    stream: &mut TcpStream,
    name: &str,
    value: &str,
) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(name.len() + value.len() + 2);
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(value.as_bytes());
    buf.push(0);
    write_message(stream, backend::PARAMETER_STATUS, &buf).await
}

/// Build and write a BackendKeyData message (process ID + secret key for cancellation).
pub async fn write_backend_key_data(
    stream: &mut TcpStream,
    pid: i32,
    secret: i32,
) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(&pid.to_be_bytes());
    buf.extend_from_slice(&secret.to_be_bytes());
    write_message(stream, backend::BACKEND_KEY_DATA, &buf).await
}

/// Build and write a ReadyForQuery message.
pub async fn write_ready_for_query(stream: &mut TcpStream, status: TxStatus) -> anyhow::Result<()> {
    write_message(stream, backend::READY_FOR_QUERY, &[status.as_byte()]).await
}

/// Column description for RowDescription messages.
#[derive(Debug, Clone)]
pub struct PgColumn {
    pub name: String,
    /// Table OID (0 if not from a table).
    pub table_oid: i32,
    /// Column attribute number (0 if not from a table).
    pub col_attr: i16,
    /// Type OID.
    pub type_oid: i32,
    /// Type size (-1 for variable-length).
    pub type_size: i16,
    /// Type modifier (-1 for default).
    pub type_mod: i32,
    /// Format code (0 = text, 1 = binary).
    pub format: i16,
}

impl PgColumn {
    /// Shortcut to create a text-formatted column with no table association.
    pub fn text(name: &str, type_oid: i32, type_size: i16) -> Self {
        Self {
            name: name.to_string(),
            table_oid: 0,
            col_attr: 0,
            type_oid,
            type_size,
            type_mod: -1,
            format: 0,
        }
    }
}

/// Build and write a RowDescription message.
pub async fn write_row_description(
    stream: &mut TcpStream,
    columns: &[PgColumn],
) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        buf.extend_from_slice(col.name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&col.table_oid.to_be_bytes());
        buf.extend_from_slice(&col.col_attr.to_be_bytes());
        buf.extend_from_slice(&col.type_oid.to_be_bytes());
        buf.extend_from_slice(&col.type_size.to_be_bytes());
        buf.extend_from_slice(&col.type_mod.to_be_bytes());
        buf.extend_from_slice(&col.format.to_be_bytes());
    }
    write_message(stream, backend::ROW_DESCRIPTION, &buf).await
}

/// Build and write a DataRow message. Each value is `None` for SQL NULL or
/// `Some(bytes)` for a text-encoded value.
pub async fn write_data_row(
    stream: &mut TcpStream,
    values: &[Option<&[u8]>],
) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for val in values {
        match val {
            None => {
                buf.extend_from_slice(&(-1i32).to_be_bytes());
            }
            Some(data) => {
                buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                buf.extend_from_slice(data);
            }
        }
    }
    write_message(stream, backend::DATA_ROW, &buf).await
}

/// Build and write a CommandComplete message.
pub async fn write_command_complete(stream: &mut TcpStream, tag: &str) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(tag.len() + 1);
    buf.extend_from_slice(tag.as_bytes());
    buf.push(0);
    write_message(stream, backend::COMMAND_COMPLETE, &buf).await
}

/// Build and write an ErrorResponse message.
///
/// Fields: S = severity, V = severity (non-localized), C = SQLSTATE code,
/// M = message.
pub async fn write_error_response(
    stream: &mut TcpStream,
    severity: &str,
    code: &str,
    message: &str,
) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(64);
    // Severity (localized)
    buf.push(b'S');
    buf.extend_from_slice(severity.as_bytes());
    buf.push(0);
    // Severity (non-localized, V field introduced in PG 9.6)
    buf.push(b'V');
    buf.extend_from_slice(severity.as_bytes());
    buf.push(0);
    // SQLSTATE code
    buf.push(b'C');
    buf.extend_from_slice(code.as_bytes());
    buf.push(0);
    // Message
    buf.push(b'M');
    buf.extend_from_slice(message.as_bytes());
    buf.push(0);
    // Terminator
    buf.push(0);
    write_message(stream, backend::ERROR_RESPONSE, &buf).await
}

/// Build and write an EmptyQueryResponse message.
pub async fn write_empty_query_response(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_message(stream, backend::EMPTY_QUERY_RESPONSE, &[]).await
}

/// Build and write ParseComplete.
pub async fn write_parse_complete(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_message(stream, backend::PARSE_COMPLETE, &[]).await
}

/// Build and write BindComplete.
pub async fn write_bind_complete(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_message(stream, backend::BIND_COMPLETE, &[]).await
}

/// Build and write CloseComplete.
pub async fn write_close_complete(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_message(stream, backend::CLOSE_COMPLETE, &[]).await
}

/// Build and write a NoData message (response to Describe on a statement
/// that produces no rows).
pub async fn write_no_data(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_message(stream, backend::NO_DATA, &[]).await
}

// ---------------------------------------------------------------------------
// Common PG type OIDs
// ---------------------------------------------------------------------------

pub mod oid {
    pub const BOOL: i32 = 16;
    pub const INT8: i32 = 20;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const VARCHAR: i32 = 1043;
    pub const TIMESTAMP: i32 = 1114;
    pub const TIMESTAMPTZ: i32 = 1184;
    pub const DATE: i32 = 1082;
    pub const TIME: i32 = 1083;
    pub const NUMERIC: i32 = 1700;
    pub const JSON: i32 = 114;
    pub const JSONB: i32 = 3802;
    pub const BYTEA: i32 = 17;
    pub const UUID: i32 = 2950;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read a C-string (null-terminated) from `buf` starting at `*pos`, advancing
/// `*pos` past the terminator.
fn read_cstring(buf: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < buf.len() && buf[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&buf[start..*pos]).to_string();
    if *pos < buf.len() {
        *pos += 1; // skip the null terminator
    }
    s
}

/// Read a C-string from payload at position, returning the string and new offset.
pub fn read_cstring_from(payload: &[u8], offset: usize) -> (String, usize) {
    let mut pos = offset;
    let s = read_cstring(payload, &mut pos);
    (s, pos)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_status_byte_roundtrip() {
        assert_eq!(TxStatus::Idle.as_byte(), b'I');
        assert_eq!(TxStatus::InTransaction.as_byte(), b'T');
        assert_eq!(TxStatus::Failed.as_byte(), b'E');
    }

    #[test]
    fn pg_column_text_shortcut() {
        let col = PgColumn::text("id", oid::INT4, 4);
        assert_eq!(col.name, "id");
        assert_eq!(col.type_oid, oid::INT4);
        assert_eq!(col.type_size, 4);
        assert_eq!(col.table_oid, 0);
        assert_eq!(col.format, 0);
    }

    #[test]
    fn read_cstring_basic() {
        let buf = b"hello\0world\0";
        let mut pos = 0;
        assert_eq!(read_cstring(buf, &mut pos), "hello");
        assert_eq!(pos, 6);
        assert_eq!(read_cstring(buf, &mut pos), "world");
        assert_eq!(pos, 12);
    }

    #[test]
    fn read_cstring_empty() {
        let buf = b"\0rest";
        let mut pos = 0;
        assert_eq!(read_cstring(buf, &mut pos), "");
        assert_eq!(pos, 1);
    }

    #[test]
    fn protocol_constants() {
        assert_eq!(PG_PROTOCOL_V3, 3 << 16);
        assert_eq!(PG_SSL_REQUEST_CODE, 80877103);
        assert_eq!(PG_CANCEL_REQUEST_CODE, 80877102);
    }

    #[test]
    fn parse_query_strips_null() {
        let payload = b"SELECT 1\0";
        assert_eq!(parse_query(payload), "SELECT 1");
    }

    #[test]
    fn parse_query_no_null() {
        let payload = b"SELECT 1";
        assert_eq!(parse_query(payload), "SELECT 1");
    }

    #[test]
    fn row_description_encoding() {
        // Verify that we can build the byte layout manually for a single column
        let col = PgColumn::text("name", oid::TEXT, -1);
        let mut buf = Vec::new();
        buf.extend_from_slice(&(1i16).to_be_bytes()); // 1 column
        buf.extend_from_slice(b"name\0");
        buf.extend_from_slice(&0i32.to_be_bytes()); // table_oid
        buf.extend_from_slice(&0i16.to_be_bytes()); // col_attr
        buf.extend_from_slice(&oid::TEXT.to_be_bytes()); // type_oid
        buf.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
        buf.extend_from_slice(&0i16.to_be_bytes()); // format (text)

        // Build via our helper for comparison
        let mut expected = Vec::new();
        expected.extend_from_slice(&(1i16).to_be_bytes());
        expected.extend_from_slice(col.name.as_bytes());
        expected.push(0);
        expected.extend_from_slice(&col.table_oid.to_be_bytes());
        expected.extend_from_slice(&col.col_attr.to_be_bytes());
        expected.extend_from_slice(&col.type_oid.to_be_bytes());
        expected.extend_from_slice(&col.type_size.to_be_bytes());
        expected.extend_from_slice(&col.type_mod.to_be_bytes());
        expected.extend_from_slice(&col.format.to_be_bytes());

        assert_eq!(buf, expected);
    }

    #[test]
    fn data_row_null_encoding() {
        // NULL is encoded as -1 (4 bytes BE)
        let null_bytes = (-1i32).to_be_bytes();
        assert_eq!(null_bytes, [0xff, 0xff, 0xff, 0xff]);
    }

    #[test]
    fn error_response_field_tags() {
        // Verify field tag bytes match PG spec
        assert_eq!(b'S', 0x53); // Severity
        assert_eq!(b'V', 0x56); // Severity (non-localized)
        assert_eq!(b'C', 0x43); // Code (SQLSTATE)
        assert_eq!(b'M', 0x4D); // Message
    }

    #[test]
    fn oid_values_match_postgres() {
        assert_eq!(oid::BOOL, 16);
        assert_eq!(oid::INT8, 20);
        assert_eq!(oid::INT4, 23);
        assert_eq!(oid::TEXT, 25);
        assert_eq!(oid::FLOAT4, 700);
        assert_eq!(oid::FLOAT8, 701);
        assert_eq!(oid::VARCHAR, 1043);
        assert_eq!(oid::TIMESTAMP, 1114);
        assert_eq!(oid::DATE, 1082);
        assert_eq!(oid::JSON, 114);
        assert_eq!(oid::JSONB, 3802);
        assert_eq!(oid::UUID, 2950);
    }

    #[tokio::test]
    async fn write_and_read_message_roundtrip() {
        // Set up a TCP pair using tokio
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            // Read the message sent by server
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::COMMAND_COMPLETE);
            let cmd = parse_query(&msg.payload); // reuse C-string parser
            assert_eq!(cmd, "SELECT 1");
        });

        let (mut server, _) = listener.accept().await.unwrap();
        write_command_complete(&mut server, "SELECT 1")
            .await
            .unwrap();

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn write_error_response_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::ERROR_RESPONSE);
            // Parse fields from error response
            let mut pos = 0;
            let mut fields = HashMap::new();
            while pos < msg.payload.len() {
                let field_type = msg.payload[pos];
                pos += 1;
                if field_type == 0 {
                    break;
                }
                let (value, new_pos) = read_cstring_from(&msg.payload, pos);
                pos = new_pos;
                fields.insert(field_type, value);
            }
            assert_eq!(fields.get(&b'S').unwrap(), "ERROR");
            assert_eq!(fields.get(&b'C').unwrap(), "42P01");
            assert_eq!(fields.get(&b'M').unwrap(), "table not found");
        });

        let (mut server, _) = listener.accept().await.unwrap();
        write_error_response(&mut server, "ERROR", "42P01", "table not found")
            .await
            .unwrap();

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn write_ready_for_query_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::READY_FOR_QUERY);
            assert_eq!(msg.payload.len(), 1);
            assert_eq!(msg.payload[0], b'I');
        });

        let (mut server, _) = listener.accept().await.unwrap();
        write_ready_for_query(&mut server, TxStatus::Idle)
            .await
            .unwrap();

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn write_data_row_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::DATA_ROW);
            // Parse: 2-byte column count, then for each column: 4-byte len + data
            let col_count = i16::from_be_bytes([msg.payload[0], msg.payload[1]]);
            assert_eq!(col_count, 3);
            let mut pos = 2;
            // Column 0: "hello"
            let len0 = i32::from_be_bytes([
                msg.payload[pos],
                msg.payload[pos + 1],
                msg.payload[pos + 2],
                msg.payload[pos + 3],
            ]);
            pos += 4;
            assert_eq!(len0, 5);
            assert_eq!(&msg.payload[pos..pos + 5], b"hello");
            pos += 5;
            // Column 1: NULL
            let len1 = i32::from_be_bytes([
                msg.payload[pos],
                msg.payload[pos + 1],
                msg.payload[pos + 2],
                msg.payload[pos + 3],
            ]);
            pos += 4;
            assert_eq!(len1, -1);
            // Column 2: "42"
            let len2 = i32::from_be_bytes([
                msg.payload[pos],
                msg.payload[pos + 1],
                msg.payload[pos + 2],
                msg.payload[pos + 3],
            ]);
            pos += 4;
            assert_eq!(len2, 2);
            assert_eq!(&msg.payload[pos..pos + 2], b"42");
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let vals: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"42")];
        write_data_row(&mut server, &vals).await.unwrap();

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn startup_message_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            // Build a startup message manually
            let mut payload = Vec::new();
            payload.extend_from_slice(&PG_PROTOCOL_V3.to_be_bytes());
            payload.extend_from_slice(b"user\0skein\0");
            payload.extend_from_slice(b"database\0testdb\0");
            payload.push(0); // terminator
            let len = (payload.len() + 4) as i32;
            client.write_i32(len).await.unwrap();
            client.write_all(&payload).await.unwrap();
            client.flush().await.unwrap();
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let msg = read_startup_message(&mut server).await.unwrap();
        let msg = msg.expect("should be a real startup message, not SSLRequest");
        assert_eq!(msg.protocol_version, PG_PROTOCOL_V3);
        assert_eq!(msg.user(), Some("skein"));
        assert_eq!(msg.database(), Some("testdb"));

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn ssl_request_returns_none() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            // Send an SSLRequest: length=8, code=80877103
            let len: i32 = 8;
            client.write_i32(len).await.unwrap();
            client.write_i32(PG_SSL_REQUEST_CODE).await.unwrap();
            client.flush().await.unwrap();
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let result = read_startup_message(&mut server).await.unwrap();
        assert!(result.is_none(), "SSLRequest should return None");

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn parameter_status_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::PARAMETER_STATUS);
            let (name, pos) = read_cstring_from(&msg.payload, 0);
            let (value, _) = read_cstring_from(&msg.payload, pos);
            assert_eq!(name, "server_version");
            assert_eq!(value, PG_SERVER_VERSION);
        });

        let (mut server, _) = listener.accept().await.unwrap();
        write_parameter_status(&mut server, "server_version", PG_SERVER_VERSION)
            .await
            .unwrap();

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn backend_key_data_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::BACKEND_KEY_DATA);
            assert_eq!(msg.payload.len(), 8);
            let pid = i32::from_be_bytes([
                msg.payload[0],
                msg.payload[1],
                msg.payload[2],
                msg.payload[3],
            ]);
            let secret = i32::from_be_bytes([
                msg.payload[4],
                msg.payload[5],
                msg.payload[6],
                msg.payload[7],
            ]);
            assert_eq!(pid, 42);
            assert_eq!(secret, 12345);
        });

        let (mut server, _) = listener.accept().await.unwrap();
        write_backend_key_data(&mut server, 42, 12345)
            .await
            .unwrap();

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn row_description_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::ROW_DESCRIPTION);
            let col_count = i16::from_be_bytes([msg.payload[0], msg.payload[1]]);
            assert_eq!(col_count, 2);
            // Parse first column name
            let (name1, pos) = read_cstring_from(&msg.payload, 2);
            assert_eq!(name1, "id");
            // Skip table_oid(4) + col_attr(2) + type_oid(4) + type_size(2) + type_mod(4) + format(2) = 18 bytes
            let pos2 = pos + 18;
            let (name2, _) = read_cstring_from(&msg.payload, pos2);
            assert_eq!(name2, "name");
        });

        let (mut server, _) = listener.accept().await.unwrap();
        let cols = vec![
            PgColumn::text("id", oid::INT4, 4),
            PgColumn::text("name", oid::TEXT, -1),
        ];
        write_row_description(&mut server, &cols).await.unwrap();

        client_handle.await.unwrap();
    }
}
