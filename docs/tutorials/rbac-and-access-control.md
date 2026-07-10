# RBAC and access control

By default the HTTP RPC/admin API uses a single shared bearer token (`SKEINDB_TOKEN`): a caller either has full access or none. This tutorial turns on **role-based access control (RBAC)** and hands out least-privilege credentials — read-only tokens, database-scoped app tokens, and per-database users.

Prerequisite: [Quickstart](quickstart.html) completed, `skeindb` on `$PATH`. All examples use the SkeinQL RPC endpoint `POST /api/v1/rpc`.

## 1. Turn on RBAC

RBAC is opt-in. Set a bootstrap superuser token and enable enforcement, then start the server:

```bash
export SKEINDB_TOKEN='super-secret-admin-token'   # the bootstrap superuser
export SKEINDB_RBAC=1                              # enforce per-principal authorization
skeindb serve --data ./data --http 8080
```

With RBAC on, **every** request must present `Authorization: Bearer <credential>`. The `SKEINDB_TOKEN` value is the **superuser** (all privileges). A request with no/unknown credential gets `401`; an authenticated caller lacking the required privilege gets `403`.

> When `SKEINDB_RBAC` is unset (the default), the auth path is exactly the legacy single-token behavior — nothing below applies.

A quick sanity check (superuser can do anything):

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'Authorization: Bearer super-secret-admin-token' \
  -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"schema.create_database","params":{"db":"analytics"}}'
```

## 2. Roles and privileges

Privileges are ordered `read < write < admin` (higher implies lower). Every method maps to a required privilege:

| Privilege | Example methods |
|---|---|
| `read` | `query.select`, `data.get`, `schema.list_tables`, `stats.*` |
| `write` | `data.insert/update/delete`, `schema.create_table`, `merge.apply` |
| `admin` | `admin.user.*`, `security.token.*`, `cluster.*` mutations, `settings.set`, `settings.encryption.*` |

Roles map to a privilege: `admin` → admin, `readwrite` → write, `readonly` → read. Unknown roles fail safe to `read`.

## 3. Issue a read-only API token

Mint a token for a dashboard that should only read:

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'Authorization: Bearer super-secret-admin-token' \
  -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":2,"method":"security.token.create",
       "params":{"role":"readonly","label":"dashboard"}}'
# → { "token_id":"tok_…", "secret":"sk_…", "role":"readonly", … }   (secret shown once)
```

That `sk_…` secret can run `query.select` but a `data.insert` returns `403 forbidden`.

## 4. Scope a token to specific databases

An app token can be confined to a set of databases with `db_scope`:

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'Authorization: Bearer super-secret-admin-token' \
  -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":3,"method":"security.token.create",
       "params":{"role":"readwrite","label":"analytics-app","db_scope":["analytics"]}}'
```

This token may read and write the `analytics` database only. Every method's target database(s) are extracted — writes and `sql.exec` from their target, `query.select` by walking joins / subqueries / set-operations / CTEs — and every one must be in scope, otherwise `403`. A cross-database `query.select` that touches an out-of-scope database is denied and names it. Scope-neutral methods (`system.ping`, `tx.begin/commit/rollback`) are always allowed; global/control-plane methods are denied for a scoped token.

## 5. Create a database user with per-database grants

Tokens are anonymous credentials; a **database user** is a named principal whose access is governed by its role (the ceiling) and its per-database grants (the allowlist).

Create the user — the login `secret` is returned **once**:

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'Authorization: Bearer super-secret-admin-token' \
  -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":4,"method":"admin.user.create",
       "params":{"username":"alice","role":"readwrite"}}'
# → { "username":"alice", "role":"readwrite", "secret":"usr_…", "grants":{} }
```

Grant `alice` write on `analytics` and read on `reporting`:

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc -H 'Authorization: Bearer super-secret-admin-token' \
  -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":5,"method":"admin.user.grant","params":{"username":"alice","db":"analytics","privileges":["write"]}}'

curl -s http://127.0.0.1:8080/api/v1/rpc -H 'Authorization: Bearer super-secret-admin-token' \
  -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":6,"method":"admin.user.grant","params":{"username":"alice","db":"reporting","privileges":["read"]}}'
```

Now `alice` logs in with her secret and is confined to those databases:

```bash
# Allowed: write to analytics
curl -s http://127.0.0.1:8080/api/v1/rpc -H 'Authorization: Bearer usr_…' -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":7,"method":"data.insert","params":{"into":{"db":"analytics","table":"events"},"rows":[]}}'

# Denied (403): write to reporting (only read granted), or anything on production (no grant),
# or any admin.* method (readwrite role < admin).
```

The effective privilege on a database is `min(role, grant)`. An `admin`-role user is unrestricted (like the superuser). `admin.user.list` never returns secrets — only a `has_secret` flag.

## 6. Recap of the decision

For each authenticated request:

1. Resolve the bearer credential to a principal (superuser token / API token / user).
2. Check the **role/global privilege** against the method's required privilege.
3. For a **scoped token**, require every target database in `db_scope`.
4. For a **database user**, require a grant covering each target database at the needed privilege.

Anything that fails returns `403 forbidden` (logged at WARN); an unknown credential returns `401`.

## Next

- [Configuration → RBAC](configuration.html#rbac-role-based-access-control-on-the-rpc-path) — the full reference (method classification, grant strings, follow-ons).
- [Admin console tour](admin-tour.html) — manage tokens and users from the web UI.
- [Setting up a 3-node cluster](setting-up-cluster.html) — with RBAC on, inter-node calls authenticate with `SKEINDB_TOKEN`.
