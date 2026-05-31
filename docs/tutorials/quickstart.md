# Quickstart — 5 minutes

Get a local SkeinDB running, run your first query, and open the admin console — in under five minutes.

## 1. Install

### macOS (Homebrew)

```bash
brew tap pinkysworld/skeindb https://github.com/pinkysworld/SkeinDB
brew install --HEAD pinkysworld/skeindb/skeindb
```

### Linux / from source (anywhere Rust runs)

```bash
git clone https://github.com/pinkysworld/SkeinDB.git
cd SkeinDB
cargo build --release
# binary is at ./target/release/skeindb
```

Prerequisite: Rust 1.75+ stable. See [Configuration](configuration.html) for other options.

## 2. Start the server

```bash
skeindb serve --data ./data --http 8080 --mysql 3306
```

Three surfaces are now open:

| Surface | URL / Port | Purpose |
|---|---|---|
| HTTP / SkeinQL | `http://127.0.0.1:8080` | Native typed RPC (JSON-RPC) |
| MySQL wire | `127.0.0.1:3306` | Connect with any MySQL client |
| Admin console | `http://127.0.0.1:8080/admin` | Web UI (SkeinAdmin) |

## 3. Run your first query

Pick your favourite path:

### A. Native SkeinQL (curl)

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"system.ping","params":{}}'
```

You should see `{"id":1,"ok":true,"result":{"pong":true,...}}`.

Continue with [Your first query](first-query.html) to create a table and insert rows.

### B. MySQL client

```bash
mysql -h 127.0.0.1 -P 3306 -u root -e "SELECT VERSION();"
```

Continue with [MySQL in 5 minutes](mysql-in-5-min.html).

### C. Admin console

Open `http://127.0.0.1:8080/admin` in your browser. Continue with [Admin console tour](admin-tour.html).

## 4. What next?

- [Your first query (SkeinQL)](first-query.html)
- [MySQL in 5 minutes](mysql-in-5-min.html)
- [Admin console tour](admin-tour.html)
- [Setting up a 3-node cluster](setting-up-cluster.html)
- [Configuration reference](configuration.html)

> **Stuck?** Check the [True Status Matrix](true-status-matrix.html) for what is currently hardened and what is still in-progress, or open an issue on [GitHub](https://github.com/pinkysworld/SkeinDB/issues).
