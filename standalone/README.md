# Queue Work Go — standalone experimental CLI

This is an **isolated Go module** so the existing MariaDB worker and its
dependencies stay unchanged. It uses SQLite as a local durable queue database.

## Build

Requires Go, a C compiler and CGO enabled for the `go-sqlite3` driver.

```bash
cd standalone
go mod tidy
CGO_ENABLED=1 go build -o queue-standalone .
```

The first `go mod tidy` downloads dependencies and generates this submodule's
`go.sum`. Review that file and commit it before a release.

## Commands

```bash
./queue-standalone enqueue -db ./queue.db -queue default -name jobs/example -payload hello
./queue-standalone claim -db ./queue.db -queue default -lease 30s
./queue-standalone renew -db ./queue.db -id TASK_ID -token CLAIM_TOKEN -lease 30s
./queue-standalone complete -db ./queue.db -id TASK_ID -token CLAIM_TOKEN
./queue-standalone fail -db ./queue.db -id TASK_ID -token CLAIM_TOKEN -reason failed
```

Each invocation opens and closes the same local SQLite file. A claim returns
a token; a different token or an expired lease cannot complete the task.
The CLI **does not execute Go plugins**, run a persistent worker, or provide a
dashboard yet. It only exercises the storage engine.

Do not point this CLI at a production database or run on shared network storage.
No MariaDB schema or data is modified.

## Local HTTP API (experimental)

Run with a strong, random secret (minimum 32 characters):

```bash
export QUEUE_API_KEY='replace-with-a-random-secret-of-at-least-32-characters'
./queue-standalone serve -db ./queue.db -listen 127.0.0.1:8088
```

Enqueue a task:

```bash
curl -X POST http://127.0.0.1:8088/v1/tasks \
  -H "Authorization: Bearer $QUEUE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"queue":"default","name":"jobs/example","payload":"hello"}'
```

The endpoints are `POST /v1/tasks`, `POST /v1/claim`,
`POST /v1/renew`, `POST /v1/complete`, and `POST /v1/fail`.
`GET /healthz` reports HTTP process health, not full storage health.

The API only listens on loopback and uses a bearer token. Do not expose it
directly to the internet. Use a properly configured TLS reverse proxy and
access control for remote deployments.

**Important:** this is a queue storage API, not yet a background worker
that executes plugins automatically. A client must claim and acknowledge jobs.
