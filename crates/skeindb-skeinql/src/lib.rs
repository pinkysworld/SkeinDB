//! SkeinQL v1.0 (scaffold)
//!
//! This crate defines request/response envelope types and core AST/value types
//! for the SkeinQL protocol.
//!
//! The normative protocol specification is in `docs/SKEINQL.md`.

use serde::{Deserialize, Serialize};

pub mod methods;
pub mod offline;
pub mod types;

pub const SKEINQL_VERSION: &str = "1.0";

/// JSON-RPC style id.
///
/// SkeinQL allows string or integer ids (and notifications with no id).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    Str(String),
    Int(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest<T = serde_json::Value> {
    /// Protocol version, e.g. "1.0".
    pub skeinql: String,

    /// Optional request id (echoed back in response).
    ///
    /// If omitted, the request is a notification and SHOULD NOT produce a
    /// response body.
    #[serde(default)]
    pub id: Option<RpcId>,

    /// Method name, e.g. "query.select".
    pub method: String,

    /// Method parameters.
    #[serde(default)]
    pub params: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse<T = serde_json::Value> {
    #[serde(default)]
    pub id: Option<RpcId>,
    pub ok: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    /// Stable machine-readable error code.
    pub code: String,

    /// Human-readable message.
    pub message: String,

    /// Optional structured error details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl RpcError {
    pub fn new<S1: Into<String>, S2: Into<String>>(code: S1, message: S2) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

impl<T> RpcResponse<T> {
    pub fn ok(id: Option<RpcId>, result: T) -> Self {
        Self {
            id,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: Option<RpcId>, error: RpcError) -> Self {
        Self {
            id,
            ok: false,
            result: None,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip_string_id() {
        let req = RpcRequest {
            skeinql: SKEINQL_VERSION.to_string(),
            id: Some(RpcId::Str("r1".to_string())),
            method: "system.ping".to_string(),
            params: Some(serde_json::json!({})),
        };

        let s = serde_json::to_string(&req).unwrap();
        let back: RpcRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.skeinql, "1.0");
        assert_eq!(back.method, "system.ping");
        assert_eq!(back.id, Some(RpcId::Str("r1".to_string())));
    }

    #[test]
    fn request_roundtrip_int_id() {
        let req = RpcRequest {
            skeinql: SKEINQL_VERSION.to_string(),
            id: Some(RpcId::Int(7)),
            method: "system.ping".to_string(),
            params: Some(serde_json::json!({})),
        };

        let s = serde_json::to_string(&req).unwrap();
        let back: RpcRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, Some(RpcId::Int(7)));
    }

    #[test]
    fn response_ok_roundtrip() {
        let resp = RpcResponse::ok(
            Some(RpcId::Str("r1".to_string())),
            serde_json::json!({"pong":true}),
        );
        let s = serde_json::to_string(&resp).unwrap();
        let back: RpcResponse = serde_json::from_str(&s).unwrap();
        assert!(back.ok);
        assert!(back.error.is_none());
        assert_eq!(back.id, Some(RpcId::Str("r1".to_string())));
    }

    #[test]
    fn response_err_roundtrip() {
        let resp: RpcResponse = RpcResponse::err(
            Some(RpcId::Str("r1".to_string())),
            RpcError::new("invalid_request", "oops"),
        );
        let s = serde_json::to_string(&resp).unwrap();
        let back: RpcResponse = serde_json::from_str(&s).unwrap();
        assert!(!back.ok);
        assert!(back.result.is_none());
        assert_eq!(back.error.unwrap().code, "invalid_request");
    }
}
