.PHONY: help fmt clippy test build run web-install web-dev web-build mysql-up mysql-down compat-mysql compat-skein

help:
	@echo "Targets:"
	@echo "  fmt            - cargo fmt"
	@echo "  clippy         - cargo clippy -D warnings"
	@echo "  test           - cargo test"
	@echo "  build          - cargo build --release"
	@echo "  run            - run skeindb dev server"
	@echo "  web-install    - npm install (web/console)"
	@echo "  web-dev        - npm run dev (web/console)"
	@echo "  web-build      - npm run build (web/console)"
	@echo "  mysql-up       - docker run MySQL 8 for compat tests"
	@echo "  mysql-down     - stop MySQL container"
	@echo "  compat-mysql   - run tests/compat/corpus.sql on MySQL container"
	@echo "  compat-skein   - run tests/compat/corpus.sql on SkeinDB"

fmt:
	cargo fmt

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test --all --all-features

build:
	cargo build --release

run:
	cargo run -p skeindb -- serve --data ./data --mysql 3306 --http 8080

web-install:
	cd web/console && npm install

web-dev:
	cd web/console && npm run dev

web-build:
	cd web/console && npm run build

mysql-up:
	-docker rm -f skein-mysql
	docker run --name skein-mysql -e MYSQL_ROOT_PASSWORD=root -p 3307:3306 -d mysql:8

mysql-down:
	-docker rm -f skein-mysql

compat-mysql:
	mysql -h 127.0.0.1 -P 3307 -u root -proot < tests/compat/corpus.sql

compat-skein:
	mysql -h 127.0.0.1 -P 3306 -u root -proot < tests/compat/corpus.sql
