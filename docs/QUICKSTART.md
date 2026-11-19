# Quickstart — isolated development environment

This guide is for **local evaluation only**. It does not modify systemd, install a daemon, or connect to a production database.

## Requirements

- Linux, Go matching `go.mod`, and Docker Compose
- A separate, disposable MariaDB 10.11 database
- An available port 9090

## 1. Clone and choose the experimental branch

```bash
git clone https://github.com/rafaWPP/queue-work-go.git
cd queue-work-go
git switch melhorias-seguranca-filas
```

## 2. Build without installing services

```bash
bash scripts/build-local.sh
```

The script builds the worker and the independent Hello World plugin. Unlike `build.sh`, it **does not run sudo or systemctl**.

## 3. Start a disposable database

```bash
docker compose -f compose.test.yml up -d --wait
```

This Compose file exposes MariaDB only on `127.0.0.1:3307`, and database contents are disposable.

**Do not run the integration tests against a database you want to preserve.** They create and drop `queue_tasks`.

## 4. Configure the worker

```bash
cp .env.example .env
cp config.example.json config.json
```

Edit `.env` so that its database values point **only** to the disposable MariaDB:

```dotenv
DB_HOST=127.0.0.1
DB_PORT=3307
DB_NAME=queue_test
DB_USER=queue_test
DB_PASSWORD=queue_test
PORT=9090
AUTH=replace-with-a-long-random-local-secret
```

Use a strong random `AUTH` value. These example database credentials are only for the disposable local container. The current worker loads `config.json` from the working directory.

## 5. Start the worker

```bash
./dist/queue-work-go
```

On its first start, the worker applies migrations from `migrations/`. Keep this process running in its own terminal. The panel is at `http://127.0.0.1:9090/login` (local testing only).

## 6. Enqueue a sample task

In a second terminal, with the database container running:

```bash
docker compose -f compose.test.yml exec -T mariadb \
  mariadb -uqueue_test -pqueue_test queue_test \
  -e "INSERT INTO queue_tasks (queue_name, task_name, data, status) VALUES ('default', 'examples/hello/main', '{\"message\":\"Hello from the queue!\"}', 'pending');"
```

The worker should load `examples/hello/main.so` and print the message. Plugin compatibility requires the same Go toolchain and dependency versions for worker and plugin.

## 7. Clean up

Stop the worker with Ctrl+C. Then run:

```bash
docker compose -f compose.test.yml down
```

The database contents will be discarded. This quickstart is **not** a deployment guide: do not expose the panel publicly or use the sample database credentials outside this local test.

## Known limitations

- The experimental branch is not yet a stable release.
- Worker restart and retry semantics need additional failure-injection tests.
- The panel's session security requires trusted proxy configuration and HTTPS for remote use.
- The legacy `build.sh` manages systemd; avoid it for local evaluation.
