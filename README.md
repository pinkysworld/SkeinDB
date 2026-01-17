# SkeinDB (scaffold)

SkeinDB is a **single-executable**, portable database engine concept.

Key goals:
- **MySQL wire-protocol compatibility** (drop-in for existing apps)
- A **custom storage engine** (cell-interned MVCC + optional dedup)
- Built-in **web management console** (phpMyAdmin-like)

This repository is a **starter scaffold**:
- Includes the on-disk format + IR + compatibility docs
- Includes a compatibility SQL corpus (`tests/compat/corpus.sql`)
- Includes Rust workspace skeleton crates
- Includes a web console placeholder

It is intentionally minimal so you can iterate quickly (manually or using Codex).

## Quick start (local)

### Build
```bash
cargo build
```

### Release build (standalone executable)
```bash
cargo build --release
```

Run the release binary:
```bash
./target/release/skeindb serve --data ./data --mysql 3306 --http 8080 --config ./skeindb-config.json
```

Windows:
```bat
target\\release\\skeindb.exe serve --data .\\data --mysql 3306 --http 8080 --config .\\skeindb-config.json
```

### Run (HTTP console + MySQL placeholder)
```bash
cargo run -p skeindb -- serve --data ./data --mysql 3306 --http 8080 --config ./skeindb-config.json
```

Then open the server console at:
```
http://127.0.0.1:8080/console
```

SQL Studio runs separately at:
```
http://127.0.0.1:8080/studio
```

Optional: load the bundled sample SQL into the console database on startup:
```bash
cargo run -p skeindb -- serve --load-sample
```

Note: the MySQL listener currently accepts connections but does not implement the wire protocol yet. The console database is in-memory and resets on restart.

### End-user run (after download)
macOS/Linux:
```bash
./skeindb serve --data ./data --mysql 3306 --http 8080 --config ./skeindb-config.json
```

Windows:
```bat
skeindb.exe serve --data .\\data --mysql 3306 --http 8080 --config .\\skeindb-config.json
```

### Compatibility corpus (once MySQL protocol is implemented)
```bash
mysql -h 127.0.0.1 -P 3306 -u root -proot < tests/compat/corpus.sql
```

## Docs
- `docs/SKEINIR.md` — canonical IR definition
- `docs/ON_DISK_FORMAT.md` — storage formats (WAL/segments/.run)
- `docs/COMPATIBILITY.md` — MySQL compatibility profile + testing policy
- `docs/PROJECT_BACKLOG.md` — Codex-friendly phased build plan

## License

TBD (choose MIT/Apache-2.0 later).
