# Hello World plugin

This example has no external service or customer dependencies. It implements the worker's `New([]byte) common.Job` entrypoint.

From the repository root, on Linux:

```bash
go build -buildmode=plugin -o examples/hello/main.so ./examples/hello
```

After setting up the queue database with the repository migrations, enqueue:

```sql
INSERT INTO queue_tasks (queue_name, task_name, data, status)
VALUES ('default', 'examples/hello/main', '{"message":"Hello from Queue Work Go!"}', 'pending');
```

The `task_name` value corresponds to the compiled plugin path **without** the `.so` extension.

The worker needs to be started from the repository root to resolve this relative plugin path. Use an isolated test database and verify the queue name is being polled in your `config.json`.

This example prints a message to standard output and does not send network requests or change business data.

**Note:** Go plugins must be built using a compatible Go toolchain and dependency versions as the host executable.
