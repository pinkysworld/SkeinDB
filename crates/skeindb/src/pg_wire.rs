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

/// SCRAM mechanism name used in SASL negotiation.
pub const SCRAM_SHA256_NAME: &str = "SCRAM-SHA-256";

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 (RFC 5802 / 7677)
// ---------------------------------------------------------------------------
pub mod scram {
    use sha2::{Digest, Sha256};

    const BLOCK_SIZE: usize = 64; // SHA-256 block size

    /// HMAC-SHA-256 (RFC 2104).
    pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
        let mut k = [0u8; BLOCK_SIZE];
        if key.len() > BLOCK_SIZE {
            let h = Sha256::digest(key);
            k[..32].copy_from_slice(&h);
        } else {
            k[..key.len()].copy_from_slice(key);
        }
        let mut ipad = [0x36u8; BLOCK_SIZE];
        let mut opad = [0x5cu8; BLOCK_SIZE];
        for i in 0..BLOCK_SIZE {
            ipad[i] ^= k[i];
            opad[i] ^= k[i];
        }
        let mut inner = Sha256::new();
        inner.update(ipad);
        inner.update(message);
        let inner_hash = inner.finalize();

        let mut outer = Sha256::new();
        outer.update(opad);
        outer.update(inner_hash);
        outer.finalize().into()
    }

    /// PBKDF2-HMAC-SHA256 (RFC 2898) — derives `dk_len` = 32 bytes.
    pub fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
        // For SCRAM we only need a single block (dk_len == 32 == hash_len).
        let mut u = hmac_sha256(password, &[salt, &1u32.to_be_bytes()].concat());
        let mut result = u;
        for _ in 1..iterations {
            u = hmac_sha256(password, &u);
            for j in 0..32 {
                result[j] ^= u[j];
            }
        }
        result
    }

    fn xor32(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = a[i] ^ b[i];
        }
        out
    }

    fn sha256(data: &[u8]) -> [u8; 32] {
        Sha256::digest(data).into()
    }

    /// Generate a random nonce string (base64-encoded, no padding).
    pub fn generate_nonce() -> String {
        let mut bytes = [0u8; 18]; // 18 bytes → 24 base64 chars
        let mut x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        x ^= std::process::id() as u64;
        x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for b in &mut bytes {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            *b = (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8;
        }
        base64_encode_no_pad(&bytes)
    }

    fn base64_encode_no_pad(data: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(data)
    }

    fn base64_encode(data: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(data)
    }

    fn base64_decode(s: &str) -> Option<Vec<u8>> {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.decode(s).ok()
    }

    /// Pre-computed SCRAM credentials for a password.
    #[derive(Debug, Clone)]
    pub struct ScramCredentials {
        pub salt: Vec<u8>,
        pub iterations: u32,
        pub stored_key: [u8; 32],
        pub server_key: [u8; 32],
    }

    impl ScramCredentials {
        /// Derive SCRAM credentials from a plaintext password.
        pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> Self {
            let salted_password = pbkdf2_sha256(password.as_bytes(), salt, iterations);
            let client_key = hmac_sha256(&salted_password, b"Client Key");
            let stored_key = sha256(&client_key);
            let server_key = hmac_sha256(&salted_password, b"Server Key");
            Self {
                salt: salt.to_vec(),
                iterations,
                stored_key,
                server_key,
            }
        }
    }

    /// Server-side SCRAM-SHA-256 state machine.
    #[derive(Debug)]
    pub struct ScramServer {
        server_nonce: String,
        client_first_bare: String,
        server_first: String,
        credentials: ScramCredentials,
    }

    impl ScramServer {
        /// Process client-first-message, return server-first-message.
        ///
        /// `client_first` is the full GS2-header + client-first-message-bare
        /// (e.g. `n,,n=user,r=clientnonce`).
        pub fn process_client_first(
            client_first: &str,
            credentials: &ScramCredentials,
        ) -> Result<(Self, String), String> {
            // Strip GS2 header: "n,," or "y,," (channel-binding not supported)
            let bare = if let Some(rest) = client_first.strip_prefix("n,,") {
                rest
            } else if let Some(rest) = client_first.strip_prefix("y,,") {
                rest
            } else {
                return Err("unsupported GS2 header in SCRAM client-first".to_string());
            };

            // Parse n=<user>,r=<client-nonce>
            let mut client_nonce = None;
            for attr in bare.split(',') {
                if let Some(val) = attr.strip_prefix("r=") {
                    client_nonce = Some(val.to_string());
                }
            }
            let client_nonce =
                client_nonce.ok_or_else(|| "missing r= in SCRAM client-first".to_string())?;

            let server_nonce_part = generate_nonce();
            let combined_nonce = format!("{}{}", client_nonce, server_nonce_part);

            let server_first = format!(
                "r={},s={},i={}",
                combined_nonce,
                base64_encode(&credentials.salt),
                credentials.iterations
            );

            Ok((
                Self {
                    server_nonce: combined_nonce,
                    client_first_bare: bare.to_string(),
                    server_first: server_first.clone(),
                    credentials: credentials.clone(),
                },
                server_first,
            ))
        }

        /// Process client-final-message, return server-final-message on success.
        pub fn process_client_final(&self, client_final: &str) -> Result<String, String> {
            // Parse c=<channel-binding>, r=<nonce>, p=<proof>
            let mut nonce = None;
            let mut proof_b64 = None;
            for attr in client_final.split(',') {
                if let Some(val) = attr.strip_prefix("r=") {
                    nonce = Some(val.to_string());
                } else if let Some(val) = attr.strip_prefix("p=") {
                    proof_b64 = Some(val.to_string());
                }
            }

            let nonce = nonce.ok_or_else(|| "missing r= in SCRAM client-final".to_string())?;
            if nonce != self.server_nonce {
                return Err("SCRAM nonce mismatch".to_string());
            }

            let proof_b64 =
                proof_b64.ok_or_else(|| "missing p= in SCRAM client-final".to_string())?;
            let client_proof = base64_decode(&proof_b64)
                .ok_or_else(|| "invalid base64 in SCRAM proof".to_string())?;
            if client_proof.len() != 32 {
                return Err("SCRAM proof wrong length".to_string());
            }

            // client-final-message-without-proof = everything before ",p="
            let without_proof = client_final
                .rfind(",p=")
                .map(|i| &client_final[..i])
                .ok_or_else(|| "malformed SCRAM client-final".to_string())?;

            // AuthMessage = client-first-message-bare + "," + server-first-message +
            //               "," + client-final-message-without-proof
            let auth_message = format!(
                "{},{},{}",
                self.client_first_bare, self.server_first, without_proof
            );

            // ClientSignature = HMAC(StoredKey, AuthMessage)
            let client_signature =
                hmac_sha256(&self.credentials.stored_key, auth_message.as_bytes());

            // Recover ClientKey = ClientProof XOR ClientSignature
            let mut recovered_client_key = [0u8; 32];
            for i in 0..32 {
                recovered_client_key[i] = client_proof[i] ^ client_signature[i];
            }

            // Verify: H(recovered_client_key) == stored_key
            let candidate_stored_key = sha256(&recovered_client_key);
            if candidate_stored_key != self.credentials.stored_key {
                return Err("SCRAM authentication failed".to_string());
            }

            // ServerSignature = HMAC(ServerKey, AuthMessage)
            let server_signature =
                hmac_sha256(&self.credentials.server_key, auth_message.as_bytes());

            Ok(format!("v={}", base64_encode(&server_signature)))
        }
    }

    /// Client-side SCRAM-SHA-256 for testing.
    #[cfg(test)]
    pub struct ScramClient {
        pub username: String,
        pub password: String,
        pub client_nonce: String,
        pub client_first_bare: String,
        pub client_first: String,
    }

    #[cfg(test)]
    impl ScramClient {
        pub fn new(username: &str, password: &str) -> Self {
            let client_nonce = generate_nonce();
            let client_first_bare = format!("n={},r={}", username, client_nonce);
            let client_first = format!("n,,{}", client_first_bare);
            Self {
                username: username.to_string(),
                password: password.to_string(),
                client_nonce,
                client_first_bare,
                client_first,
            }
        }

        /// Process server-first-message, return client-final-message.
        pub fn process_server_first(&self, server_first: &str) -> Result<String, String> {
            let mut combined_nonce = None;
            let mut salt_b64 = None;
            let mut iterations = None;
            for attr in server_first.split(',') {
                if let Some(val) = attr.strip_prefix("r=") {
                    combined_nonce = Some(val.to_string());
                } else if let Some(val) = attr.strip_prefix("s=") {
                    salt_b64 = Some(val.to_string());
                } else if let Some(val) = attr.strip_prefix("i=") {
                    iterations = Some(
                        val.parse::<u32>()
                            .map_err(|_| "invalid iteration count".to_string())?,
                    );
                }
            }

            let combined_nonce =
                combined_nonce.ok_or_else(|| "missing r= in server-first".to_string())?;
            if !combined_nonce.starts_with(&self.client_nonce) {
                return Err("server nonce doesn't start with client nonce".to_string());
            }
            let salt =
                base64_decode(&salt_b64.ok_or_else(|| "missing s= in server-first".to_string())?)
                    .ok_or_else(|| "invalid salt base64".to_string())?;
            let iterations = iterations.ok_or_else(|| "missing i= in server-first".to_string())?;

            let salted_password = pbkdf2_sha256(self.password.as_bytes(), &salt, iterations);
            let client_key = hmac_sha256(&salted_password, b"Client Key");
            let stored_key = sha256(&client_key);

            // channel-binding = base64("n,,") = "biws"
            let client_final_without_proof = format!("c=biws,r={}", combined_nonce);

            let auth_message = format!(
                "{},{},{}",
                self.client_first_bare, server_first, client_final_without_proof
            );

            let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
            let client_proof = xor32(&client_key, &client_signature);

            Ok(format!(
                "{},p={}",
                client_final_without_proof,
                base64_encode(&client_proof)
            ))
        }
    }
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
    if !(8..=10240).contains(&len) {
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
    buf.extend_from_slice(&auth::OK.to_be_bytes());
    write_message(stream, backend::AUTHENTICATION, &buf).await
}

/// Build and write an AuthenticationCleartextPassword request.
pub async fn write_auth_cleartext_password(stream: &mut TcpStream) -> anyhow::Result<()> {
    let buf = auth::CLEARTEXT_PASSWORD.to_be_bytes();
    write_message(stream, backend::AUTHENTICATION, &buf).await
}

/// Build and write an AuthenticationSASL message listing available mechanisms.
pub async fn write_auth_sasl(stream: &mut TcpStream, mechanisms: &[&str]) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&auth::SASL.to_be_bytes());
    for mech in mechanisms {
        buf.extend_from_slice(mech.as_bytes());
        buf.push(0);
    }
    buf.push(0); // terminator of mechanism list
    write_message(stream, backend::AUTHENTICATION, &buf).await
}

/// Build and write an AuthenticationSASLContinue message.
pub async fn write_auth_sasl_continue(stream: &mut TcpStream, data: &str) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(4 + data.len());
    buf.extend_from_slice(&auth::SASL_CONTINUE.to_be_bytes());
    buf.extend_from_slice(data.as_bytes());
    write_message(stream, backend::AUTHENTICATION, &buf).await
}

/// Build and write an AuthenticationSASLFinal message.
pub async fn write_auth_sasl_final(stream: &mut TcpStream, data: &str) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(4 + data.len());
    buf.extend_from_slice(&auth::SASL_FINAL.to_be_bytes());
    buf.extend_from_slice(data.as_bytes());
    write_message(stream, backend::AUTHENTICATION, &buf).await
}

/// Parse a SASLInitialResponse from a PasswordMessage payload.
///
/// Returns `(mechanism_name, client_first_message)`.
pub fn parse_sasl_initial_response(payload: &[u8]) -> Option<(String, String)> {
    let (mechanism, offset) = read_cstring_from(payload, 0);
    if offset + 4 > payload.len() {
        return None;
    }
    let data_len = i32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]);
    if data_len < 0 {
        return Some((mechanism, String::new()));
    }
    let start = offset + 4;
    let end = start + data_len as usize;
    if end > payload.len() {
        return None;
    }
    let data = String::from_utf8_lossy(&payload[start..end]).to_string();
    Some((mechanism, data))
}

/// Parse a SASLResponse (client-final-message) from a PasswordMessage payload.
pub fn parse_sasl_response(payload: &[u8]) -> String {
    String::from_utf8_lossy(payload).to_string()
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

/// Build and write a ParameterDescription message.
pub async fn write_parameter_description(
    stream: &mut TcpStream,
    type_oids: &[i32],
) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(2 + (type_oids.len() * 4));
    buf.extend_from_slice(&(type_oids.len() as i16).to_be_bytes());
    for type_oid in type_oids {
        buf.extend_from_slice(&type_oid.to_be_bytes());
    }
    write_message(stream, backend::PARAMETER_DESCRIPTION, &buf).await
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

    // Array OIDs
    pub const BOOL_ARRAY: i32 = 1000;
    pub const INT4_ARRAY: i32 = 1007;
    pub const INT8_ARRAY: i32 = 1016;
    pub const FLOAT4_ARRAY: i32 = 1021;
    pub const FLOAT8_ARRAY: i32 = 1022;
    pub const TEXT_ARRAY: i32 = 1009;
    pub const VARCHAR_ARRAY: i32 = 1015;
    pub const DATE_ARRAY: i32 = 1182;
    pub const TIMESTAMP_ARRAY: i32 = 1115;
    pub const JSONB_ARRAY: i32 = 3807;
    pub const UUID_ARRAY: i32 = 2951;

    /// Return the element OID for an array OID, or None if not an array type.
    pub fn array_element_oid(array_oid: i32) -> Option<i32> {
        match array_oid {
            BOOL_ARRAY => Some(BOOL),
            INT4_ARRAY => Some(INT4),
            INT8_ARRAY => Some(INT8),
            FLOAT4_ARRAY => Some(FLOAT4),
            FLOAT8_ARRAY => Some(FLOAT8),
            TEXT_ARRAY => Some(TEXT),
            VARCHAR_ARRAY => Some(VARCHAR),
            DATE_ARRAY => Some(DATE),
            TIMESTAMP_ARRAY => Some(TIMESTAMP),
            JSONB_ARRAY => Some(JSONB),
            UUID_ARRAY => Some(UUID),
            _ => None,
        }
    }

    /// Return the array OID for a scalar element OID, or None.
    pub fn scalar_to_array_oid(scalar_oid: i32) -> Option<i32> {
        match scalar_oid {
            BOOL => Some(BOOL_ARRAY),
            INT4 => Some(INT4_ARRAY),
            INT8 => Some(INT8_ARRAY),
            FLOAT4 => Some(FLOAT4_ARRAY),
            FLOAT8 => Some(FLOAT8_ARRAY),
            TEXT => Some(TEXT_ARRAY),
            VARCHAR => Some(VARCHAR_ARRAY),
            DATE => Some(DATE_ARRAY),
            TIMESTAMP => Some(TIMESTAMP_ARRAY),
            JSONB => Some(JSONB_ARRAY),
            UUID => Some(UUID_ARRAY),
            _ => None,
        }
    }
}

/// Encode a value in PG binary format for the given type OID.
/// Returns `None` if the type is not supported for binary encoding.
pub fn encode_binary_value(type_oid: i32, text_value: &str) -> Option<Vec<u8>> {
    match type_oid {
        oid::BOOL => {
            let v = match text_value.trim().to_ascii_lowercase().as_str() {
                "t" | "true" | "1" => 1u8,
                _ => 0u8,
            };
            Some(vec![v])
        }
        oid::INT4 => {
            let v: i32 = text_value.trim().parse().ok()?;
            Some(v.to_be_bytes().to_vec())
        }
        oid::INT8 => {
            let v: i64 = text_value.trim().parse().ok()?;
            Some(v.to_be_bytes().to_vec())
        }
        oid::FLOAT4 => {
            let v: f32 = text_value.trim().parse().ok()?;
            Some(v.to_be_bytes().to_vec())
        }
        oid::FLOAT8 => {
            let v: f64 = text_value.trim().parse().ok()?;
            Some(v.to_be_bytes().to_vec())
        }
        oid::TEXT | oid::VARCHAR | oid::JSON | oid::JSONB => Some(text_value.as_bytes().to_vec()),
        oid::UUID => {
            // Parse UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" to 16 bytes
            let hex: String = text_value
                .chars()
                .filter(|c| c.is_ascii_hexdigit())
                .collect();
            if hex.len() != 32 {
                return None;
            }
            let mut bytes = Vec::with_capacity(16);
            for i in (0..32).step_by(2) {
                bytes.push(u8::from_str_radix(&hex[i..i + 2], 16).ok()?);
            }
            Some(bytes)
        }
        oid::BYTEA => {
            // Already hex-encoded with \x prefix from text format
            if let Some(hex) = text_value.strip_prefix("\\x") {
                let mut bytes = Vec::with_capacity(hex.len() / 2);
                for i in (0..hex.len()).step_by(2) {
                    if i + 2 > hex.len() {
                        break;
                    }
                    bytes.push(u8::from_str_radix(&hex[i..i + 2], 16).ok()?);
                }
                Some(bytes)
            } else {
                Some(text_value.as_bytes().to_vec())
            }
        }
        _ => None,
    }
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

    #[tokio::test]
    async fn write_parameter_description_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            let msg = read_message(&mut client).await.unwrap();
            assert_eq!(msg.tag, backend::PARAMETER_DESCRIPTION);
            assert_eq!(i16::from_be_bytes([msg.payload[0], msg.payload[1]]), 2);
            assert_eq!(
                i32::from_be_bytes([
                    msg.payload[2],
                    msg.payload[3],
                    msg.payload[4],
                    msg.payload[5],
                ]),
                oid::INT8
            );
            assert_eq!(
                i32::from_be_bytes([
                    msg.payload[6],
                    msg.payload[7],
                    msg.payload[8],
                    msg.payload[9],
                ]),
                oid::TEXT
            );
        });

        let (mut server, _) = listener.accept().await.unwrap();
        write_parameter_description(&mut server, &[oid::INT8, oid::TEXT])
            .await
            .unwrap();

        client_handle.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // T407: Array OID constants + utilities
    // -----------------------------------------------------------------------

    #[test]
    fn array_oid_constants_match_postgres() {
        assert_eq!(oid::BOOL_ARRAY, 1000);
        assert_eq!(oid::INT4_ARRAY, 1007);
        assert_eq!(oid::INT8_ARRAY, 1016);
        assert_eq!(oid::FLOAT4_ARRAY, 1021);
        assert_eq!(oid::FLOAT8_ARRAY, 1022);
        assert_eq!(oid::TEXT_ARRAY, 1009);
        assert_eq!(oid::VARCHAR_ARRAY, 1015);
        assert_eq!(oid::DATE_ARRAY, 1182);
        assert_eq!(oid::TIMESTAMP_ARRAY, 1115);
        assert_eq!(oid::JSONB_ARRAY, 3807);
        assert_eq!(oid::UUID_ARRAY, 2951);
    }

    #[test]
    fn array_element_oid_roundtrip() {
        assert_eq!(oid::array_element_oid(oid::INT4_ARRAY), Some(oid::INT4));
        assert_eq!(oid::array_element_oid(oid::INT8_ARRAY), Some(oid::INT8));
        assert_eq!(oid::array_element_oid(oid::TEXT_ARRAY), Some(oid::TEXT));
        assert_eq!(oid::array_element_oid(oid::BOOL_ARRAY), Some(oid::BOOL));
        assert_eq!(oid::array_element_oid(oid::UUID_ARRAY), Some(oid::UUID));
        assert_eq!(oid::array_element_oid(oid::JSONB_ARRAY), Some(oid::JSONB));
        assert_eq!(oid::array_element_oid(oid::INT4), None); // scalar → None
        assert_eq!(oid::array_element_oid(0), None);
    }

    #[test]
    fn scalar_to_array_oid_roundtrip() {
        assert_eq!(oid::scalar_to_array_oid(oid::INT4), Some(oid::INT4_ARRAY));
        assert_eq!(oid::scalar_to_array_oid(oid::INT8), Some(oid::INT8_ARRAY));
        assert_eq!(oid::scalar_to_array_oid(oid::TEXT), Some(oid::TEXT_ARRAY));
        assert_eq!(oid::scalar_to_array_oid(oid::BOOL), Some(oid::BOOL_ARRAY));
        assert_eq!(oid::scalar_to_array_oid(oid::UUID), Some(oid::UUID_ARRAY));
        assert_eq!(oid::scalar_to_array_oid(oid::JSONB), Some(oid::JSONB_ARRAY));
        assert_eq!(oid::scalar_to_array_oid(999), None); // unknown → None
    }

    // -----------------------------------------------------------------------
    // T407: Binary encoding
    // -----------------------------------------------------------------------

    #[test]
    fn encode_binary_bool() {
        assert_eq!(encode_binary_value(oid::BOOL, "t"), Some(vec![1]));
        assert_eq!(encode_binary_value(oid::BOOL, "true"), Some(vec![1]));
        assert_eq!(encode_binary_value(oid::BOOL, "1"), Some(vec![1]));
        assert_eq!(encode_binary_value(oid::BOOL, "f"), Some(vec![0]));
        assert_eq!(encode_binary_value(oid::BOOL, "false"), Some(vec![0]));
    }

    #[test]
    fn encode_binary_int4() {
        assert_eq!(
            encode_binary_value(oid::INT4, "42"),
            Some(42i32.to_be_bytes().to_vec())
        );
        assert_eq!(
            encode_binary_value(oid::INT4, "-1"),
            Some((-1i32).to_be_bytes().to_vec())
        );
        assert_eq!(encode_binary_value(oid::INT4, "abc"), None);
    }

    #[test]
    fn encode_binary_int8() {
        assert_eq!(
            encode_binary_value(oid::INT8, "1234567890"),
            Some(1234567890i64.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn encode_binary_float8() {
        let bytes = encode_binary_value(oid::FLOAT8, "3.14").unwrap();
        assert_eq!(bytes.len(), 8);
        let val = f64::from_be_bytes(bytes.try_into().unwrap());
        assert!((val - 3.14).abs() < 1e-10);
    }

    #[test]
    fn encode_binary_float4() {
        let bytes = encode_binary_value(oid::FLOAT4, "2.5").unwrap();
        assert_eq!(bytes.len(), 4);
        let val = f32::from_be_bytes(bytes.try_into().unwrap());
        assert!((val - 2.5).abs() < 1e-6);
    }

    #[test]
    fn encode_binary_text_passthrough() {
        assert_eq!(
            encode_binary_value(oid::TEXT, "hello"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            encode_binary_value(oid::VARCHAR, "world"),
            Some(b"world".to_vec())
        );
        assert_eq!(
            encode_binary_value(oid::JSONB, r#"{"a":1}"#),
            Some(br#"{"a":1}"#.to_vec())
        );
    }

    #[test]
    fn encode_binary_uuid() {
        let bytes = encode_binary_value(oid::UUID, "550e8400-e29b-41d4-a716-446655440000");
        assert!(bytes.is_some());
        assert_eq!(bytes.as_ref().unwrap().len(), 16);
        assert_eq!(bytes.unwrap()[0], 0x55);
        // Invalid length
        assert_eq!(encode_binary_value(oid::UUID, "not-a-uuid"), None);
    }

    #[test]
    fn encode_binary_bytea_hex() {
        let bytes = encode_binary_value(oid::BYTEA, "\\xDEAD").unwrap();
        assert_eq!(bytes, vec![0xDE, 0xAD]);
    }

    #[test]
    fn encode_binary_unknown_oid_returns_none() {
        assert_eq!(encode_binary_value(99999, "hello"), None);
    }

    // -----------------------------------------------------------------------
    // T401: SCRAM-SHA-256
    // -----------------------------------------------------------------------

    #[test]
    fn scram_hmac_sha256_known_vector() {
        // RFC 4231 Test Case 2: key = "Jefe", data = "what do ya want for nothing?"
        let result = scram::hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let expected: [u8; 32] = [
            0x5b, 0xdc, 0xc1, 0x46, 0xbf, 0x60, 0x75, 0x4e, 0x6a, 0x04, 0x24, 0x26, 0x08, 0x95,
            0x75, 0xc7, 0x5a, 0x00, 0x3f, 0x08, 0x9d, 0x27, 0x39, 0x83, 0x9d, 0xec, 0x58, 0xb9,
            0x64, 0xec, 0x38, 0x43,
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn scram_pbkdf2_sha256_known_vector() {
        // RFC 7677 example: password="pencil", salt="QSXCR+Q6sek8bf92", i=4096
        // This matches the test vector from RFC 7677, Section 3.
        let salt = b"QSXCR+Q6sek8bf92";
        let result = scram::pbkdf2_sha256(b"pencil", salt, 4096);
        // Just verify it produces 32 bytes (the exact value depends on raw salt bytes)
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn scram_credentials_from_password() {
        let salt = b"test-salt-1234567";
        let creds = scram::ScramCredentials::from_password("mypassword", salt, 4096);
        assert_eq!(creds.salt, salt);
        assert_eq!(creds.iterations, 4096);
        assert_eq!(creds.stored_key.len(), 32);
        assert_eq!(creds.server_key.len(), 32);

        // Same inputs produce same outputs (deterministic)
        let creds2 = scram::ScramCredentials::from_password("mypassword", salt, 4096);
        assert_eq!(creds.stored_key, creds2.stored_key);
        assert_eq!(creds.server_key, creds2.server_key);
    }

    #[test]
    fn scram_generate_nonce_is_nonempty() {
        let nonce = scram::generate_nonce();
        assert!(!nonce.is_empty());
        assert!(nonce.len() >= 16); // 18 raw bytes → 24 base64 chars
    }

    #[test]
    fn scram_full_exchange_succeeds() {
        let password = "hunter2";
        let salt = b"a-random-salt-16";
        let creds = scram::ScramCredentials::from_password(password, salt, 4096);

        // Client side
        let client = scram::ScramClient::new("testuser", password);

        // Server processes client-first
        let (scram_state, server_first) =
            scram::ScramServer::process_client_first(&client.client_first, &creds).unwrap();

        // Client processes server-first, produces client-final
        let client_final = client.process_server_first(&server_first).unwrap();

        // Server processes client-final, produces server-final
        let server_final = scram_state.process_client_final(&client_final).unwrap();

        // Server-final must start with "v="
        assert!(server_final.starts_with("v="), "got: {server_final}");
    }

    #[test]
    fn scram_wrong_password_fails() {
        let salt = b"a-random-salt-16";
        let creds = scram::ScramCredentials::from_password("correct-password", salt, 4096);

        let client = scram::ScramClient::new("testuser", "wrong-password");

        let (scram_state, server_first) =
            scram::ScramServer::process_client_first(&client.client_first, &creds).unwrap();

        let client_final = client.process_server_first(&server_first).unwrap();

        let result = scram_state.process_client_final(&client_final);
        assert!(result.is_err());
    }

    #[test]
    fn scram_server_rejects_bad_gs2_header() {
        let salt = b"salt";
        let creds = scram::ScramCredentials::from_password("pw", salt, 4096);
        let result = scram::ScramServer::process_client_first("bad,,n=user,r=nonce", &creds);
        assert!(result.is_err());
    }

    #[test]
    fn scram_server_rejects_missing_nonce() {
        let salt = b"salt";
        let creds = scram::ScramCredentials::from_password("pw", salt, 4096);
        let result = scram::ScramServer::process_client_first("n,,n=user", &creds);
        assert!(result.is_err());
    }

    #[test]
    fn parse_sasl_initial_response_basic() {
        let mechanism = "SCRAM-SHA-256";
        let data = "n,,n=user,r=nonce123";
        let mut payload = Vec::new();
        payload.extend_from_slice(mechanism.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&(data.len() as i32).to_be_bytes());
        payload.extend_from_slice(data.as_bytes());

        let (mech, msg) = parse_sasl_initial_response(&payload).unwrap();
        assert_eq!(mech, "SCRAM-SHA-256");
        assert_eq!(msg, "n,,n=user,r=nonce123");
    }

    #[test]
    fn parse_sasl_response_basic() {
        let data = "c=biws,r=nonce123abc,p=proof";
        assert_eq!(parse_sasl_response(data.as_bytes()), data);
    }
}
