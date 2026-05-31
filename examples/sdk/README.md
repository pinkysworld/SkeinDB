# SkeinQL SDK & driver examples

Small, dependency-free reference drivers for the SkeinQL JSON-RPC surface
(`POST /api/v1/rpc`). Each one demonstrates the request/response envelope,
error handling (HTTP 200 may still carry an RPC error — always inspect the
envelope `ok` flag), and a couple of common method families.

See [docs/API_REFERENCE.md](../../docs/API_REFERENCE.md) and
[docs/SKEINQL.md](../../docs/SKEINQL.md) for the full contract.

## The envelope

Request:

```json
{ "skeinql": "1.0", "id": "req-1", "method": "query.select", "params": { "query": "SELECT 1 AS one" } }
```

Success: `{ "id": "req-1", "ok": true, "result": { ... } }`
Error: `{ "id": "req-1", "ok": false, "error": { "code": "...", "message": "...", "details": { ... } } }`

## Python

[`python/skeindb_client.py`](python/skeindb_client.py) — stdlib only (`urllib`).

```python
from skeindb_client import SkeinClient
client = SkeinClient("http://localhost:8080")
print(client.capabilities())
print(client.select("SELECT 1 AS one"))
```

Run the network-free tests:

```bash
python -m unittest discover examples/sdk/python
```

## Node.js

[`node/skeindb_client.mjs`](node/skeindb_client.mjs) — Node >= 18 (global `fetch`),
no dependencies.

```js
import { SkeinClient } from './skeindb_client.mjs';
const client = new SkeinClient('http://localhost:8080');
console.log(await client.capabilities());
console.log(await client.select('SELECT 1 AS one'));
```

Run the network-free tests:

```bash
node --test examples/sdk/node/
```

## curl / bash

[`bash/skeinql.sh`](bash/skeinql.sh):

```bash
./examples/sdk/bash/skeinql.sh http://localhost:8080
```

All three clients are intentionally tiny so they double as documentation; the
envelope helpers are pure functions and the HTTP transport is injectable, which
is how the included tests run without a live server.
