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

## Automatic worker (experimental)

In a second terminal, run:

```bash
./queue-standalone work -db ./queue.db -queue default -lease 30s -poll 500ms
```

The worker claims tasks, runs a **registered** handler, renews the lease
while the handler runs, and acknowledges success or failure. This first
version deliberately includes only safe demonstration handlers:

- `builtin/noop`: completes without side effects
- `builtin/log`: logs that a task was processed, without printing its payload

To exercise it:

```bash
./queue-standalone enqueue -db ./queue.db -name builtin/log -payload hello
```

Custom application jobs require registering handlers in Go; arbitrary
commands, dynamic scripts and legacy `.so` plugins are **not** executed
by this worker. The worker retries failed handlers up to 3 total attempts by default, using
exponential backoff (1s, 2s, ... capped at 1h). Configure with
\`-max-attempts\` and \`-retry-delay\`. Unknown handler names are marked
failed immediately. A dead-letter queue and operator-driven replay are not
yet implemented.

Handlers must honor context cancellation. If a worker crashes or loses its
lease, a task may execute again; external side effects must be idempotent.
This is **at-least-once**, not exactly-once, processing.

### Retry policy

```bash
./queue-standalone work -db ./queue.db -max-attempts 5 -retry-delay 2s
```

A task that fails is returned to pending with a future availability timestamp.
Once the maximum number of claims is reached, it is marked failed. Retries
preserve ownership fencing; expired workers cannot reschedule a task.

## Task inspection and manual replay

Authenticated POST endpoints (same bearer key as other operations):

- `/v1/tasks/list`: `{"queue":"default","name":"failed"}` lists up to
  100 recent tasks. Here `name` is the **status filter**, not the task name;
  omit it to include all statuses.
- `/v1/tasks/get`: `{"id":"TASK_ID"}` returns task status, attempts,
  last error and lease expiry, but not the payload or ownership token.
- `/v1/tasks/replay`: `{"id":"TASK_ID"}` moves a **failed** task back
  to pending. Replay is explicit and does not clear the attempt history.

Example:

```bash
curl -X POST http://127.0.0.1:8088/v1/tasks/list \
  -H "Authorization: Bearer $QUEUE_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","name":"failed"}'
```

Replay should only be used after resolving the underlying failure. The
API does not provide pagination or an administrative dashboard yet.
