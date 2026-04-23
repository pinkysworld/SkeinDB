# Encryption and key rotation

This guide registers database master keys, enables an encryption mode, inspects status, and rotates the active key through SkeinQL.

Prerequisite: [Your first query (SkeinQL)](first-query.html) completed.

## 1. Create a target database

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"schema.create_database","params":{"db":"app"}}' | jq
```

## 2. Generate and register the first key

```bash
KEY1=$(openssl rand -base64 32 | tr -d '\n')

curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d "{\"skeinql\":\"1.0\",\"id\":2,\"method\":\"settings.encryption.register_key\",\"params\":{\"db\":\"app\",\"key_id\":\"k1\",\"master_key_b64\":\"$KEY1\",\"make_active\":true}}" \
  | jq
```

## 3. Enable an encryption mode

Two modes are currently relevant:

- `enc_random` for randomized encryption at rest
- `enc_mle_db` for dedup-preserving convergent encryption within one database scope

Enable randomized mode:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":3,"method":"settings.encryption.set_mode","params":{"db":"app","mode":"enc_random"}}' | jq
```

If you need dedup-preserving encryption instead, switch `mode` to `enc_mle_db`.

## 4. Inspect status

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":4,"method":"settings.encryption.status","params":{"db":"app"}}' | jq
```

The response includes:

- current mode
- active key id
- registered key ids
- recent redacted audit entries

## 5. Register a new key and rotate

```bash
KEY2=$(openssl rand -base64 32 | tr -d '\n')

curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d "{\"skeinql\":\"1.0\",\"id\":5,\"method\":\"settings.encryption.register_key\",\"params\":{\"db\":\"app\",\"key_id\":\"k2\",\"master_key_b64\":\"$KEY2\",\"make_active\":false}}" \
  | jq

curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":6,"method":"settings.encryption.rotate_key","params":{"db":"app","new_key_id":"k2"}}' | jq
```

Check status again:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":7,"method":"settings.encryption.status","params":{"db":"app"}}' | jq
```

## 6. Switch the active key without a rotation workflow

If you already registered a key and only want future writes to use it:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":8,"method":"settings.encryption.set_active_key","params":{"db":"app","key_id":"k2"}}' | jq
```

## 7. Operational notes

- Master keys are accepted as base64 but are **not persisted to disk**. Re-register them after restart.
- `enc_mle_db` preserves deduplication, but leaks equality within the same database scope. That is the trade-off for convergent encryption.
- The current runtime exposes rotation state and value-store re-encryption primitives, but a fully operator-driven background rewrite pass remains follow-up work.
- SkeinAdmin exposes the same flows through its dedicated **Encryption** panel.

## Next

- [Convergent encryption](convergent-encryption.html)
- [Admin console tour](admin-tour.html)
- [True status matrix](true-status-matrix.html)