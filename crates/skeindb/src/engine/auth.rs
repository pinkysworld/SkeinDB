//! RBAC credential management: API tokens and database users.
//!
//! These `Engine` methods own the two credential stores (`api_tokens`, `db_users`) and the
//! constant-time secret resolution the HTTP RPC path uses to map a bearer credential to a
//! principal (see `server::resolve_rpc_principal`). The privilege/scope *policy* lives in
//! `server.rs`; this module only stores credentials and resolves secrets.

use super::*;

impl Engine {
    /// Create an API token, optionally restricted to a set of databases. `db_scope = None`
    /// leaves the token unrestricted across databases; `Some(list)` limits it to data-plane
    /// operations on those databases under RBAC enforcement.
    pub fn create_api_token_scoped(
        &mut self,
        role: &str,
        label: &str,
        ttl_ms: u64,
        db_scope: Option<Vec<String>>,
    ) -> ApiToken {
        let id = format!("tok_{:016x}", self.api_token_next_id);
        self.api_token_next_id += 1;
        let secret = format!("sk_{:032x}", {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            id.hash(&mut h);
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .hash(&mut h);
            h.finish()
        });
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let expires = if ttl_ms > 0 { now + ttl_ms } else { 0 };
        let token = ApiToken {
            token_id: id.clone(),
            secret,
            role: role.to_string(),
            label: label.to_string(),
            created_at_ms: now,
            expires_at_ms: expires,
            db_scope: db_scope.filter(|s| !s.is_empty()),
        };
        self.api_tokens.insert(id, token.clone());
        token
    }

    pub fn list_api_tokens(&self) -> Vec<ApiToken> {
        self.api_tokens.values().cloned().collect()
    }

    pub fn revoke_api_token(&mut self, token_id: &str) -> bool {
        self.api_tokens.remove(token_id).is_some()
    }

    /// Resolve an API-token *secret* (presented as an HTTP bearer credential) to the
    /// `(role, token_id)` of a live, non-expired token that owns it, if any. Used by RBAC
    /// enforcement on the RPC path to map a credential to a principal's role.
    ///
    /// Every stored secret is compared in constant time and the loop never breaks early, so
    /// a caller cannot learn a valid secret — or even how many tokens exist — from the time
    /// the comparison takes.
    pub fn api_token_role_for_secret(&self, secret: &str) -> Option<ApiTokenAuth> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut found: Option<ApiTokenAuth> = None;
        for tok in self.api_tokens.values() {
            let live = tok.expires_at_ms == 0 || tok.expires_at_ms > now;
            if constant_time_eq(tok.secret.as_bytes(), secret.as_bytes()) && live {
                found = Some(ApiTokenAuth {
                    role: tok.role.clone(),
                    token_id: tok.token_id.clone(),
                    db_scope: tok.db_scope.clone(),
                });
            }
        }
        found
    }

    // ── T044: User management ───────────────────────────────────────────────
    pub fn user_create(&mut self, username: &str, role: &str) -> DbUser {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let secret = format!("usr_{:032x}", {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            username.hash(&mut h);
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .hash(&mut h);
            h.finish()
        });
        let user = DbUser {
            username: username.to_string(),
            role: role.to_string(),
            created_at_ms: now,
            grants: HashMap::new(),
            secret,
        };
        self.db_users.insert(username.to_string(), user.clone());
        user
    }

    /// Resolve a DbUser login secret (presented as a bearer credential) to that user, if a user
    /// with a non-empty matching secret exists. Constant-time over all users (and never matches
    /// an empty secret) so a caller can't learn a secret — or the user count — by timing.
    pub fn user_for_secret(&self, secret: &str) -> Option<DbUser> {
        if secret.is_empty() {
            return None;
        }
        let mut found: Option<DbUser> = None;
        for user in self.db_users.values() {
            if !user.secret.is_empty()
                && constant_time_eq(user.secret.as_bytes(), secret.as_bytes())
            {
                found = Some(user.clone());
            }
        }
        found
    }

    pub fn user_list(&self) -> Vec<DbUser> {
        self.db_users.values().cloned().collect()
    }

    pub fn user_drop(&mut self, username: &str) -> bool {
        self.db_users.remove(username).is_some()
    }

    pub fn user_grant(
        &mut self,
        username: &str,
        db: &str,
        privileges: Vec<String>,
    ) -> anyhow::Result<()> {
        let user = self
            .db_users
            .get_mut(username)
            .ok_or_else(|| anyhow::anyhow!("user not found: {username}"))?;
        user.grants.insert(db.to_string(), privileges);
        Ok(())
    }

    pub fn user_revoke(&mut self, username: &str, db: &str) -> anyhow::Result<bool> {
        let user = self
            .db_users
            .get_mut(username)
            .ok_or_else(|| anyhow::anyhow!("user not found: {username}"))?;
        Ok(user.grants.remove(db).is_some())
    }
}
