# Testing and development

This guide describes a **disposable local MariaDB environment**, not a production deployment.

## Requirements

- Go 1.22.2 or newer compatible version
- Docker with Docker Compose
- Linux for compiling Go plugins

## Start a disposable MariaDB instance

```bash
docker compose -f compose.test.yml up -d --wait
```

The database is exposed only on localhost port 3307 and stores its data in a temporary filesystem. All data disappears when the container is removed.

## Run tests

```bash
export QUEUE_TEST_DSN='queue_test:queue_test@tcp(127.0.0.1:3307)/queue_test?parseTime=true'
go test -v . ./common ./web ./cmd/webtech
```

The integration test creates and drops a table named `queue_tasks`. **Never set `QUEUE_TEST_DSN` to a production or shared database.**

```bash
docker compose -f compose.test.yml down
```

## Compile the core

```bash
go build . ./common ./web ./cmd/webtech
```

## Compile plugins

The `jobs/` directory contains multiple independent Go plugins with overlapping symbols. Running `go test ./...` or `go build ./...` across all directories can fail because these source files are not intended to compile together as one package. Build plugins individually:

```bash
for file in jobs/*.go jobs/mk/*.go; do
  go build -buildmode=plugin -o /tmp/queue-plugin.so "$file" || exit 1
done
```

## Current limitations

- MariaDB 10.11 is the initial CI target, not a verified compatibility matrix.
- Concurrency tests verify task claiming, not exactly-once delivery after network failures.
- The web panel still uses a query-string secret. Do not expose it publicly.
- The test database must remain isolated from real customer information.
