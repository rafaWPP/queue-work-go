# Standalone SQLite backend (experimental)

The package `internal/queueengine/sqlite` now implements the full queue storage interface:
enqueue, atomic claim, lease renewal, completion, failure, and reopening persisted data.

**Not yet wired to the main worker or dashboard.** The current MariaDB execution
path remains unchanged. No existing data is migrated or deleted.

## Driver requirement

The backend uses Go's `database/sql` abstraction. The application embedding it
must register a compatible SQLite driver. For example, in a separate integration
program using a modern SQLite driver, register the driver and call:

```go
store, err := sqlite.Open(ctx, "sqlite", "/private/data/queue.db")
if err != nil { return err }
defer store.Close()

id, err := store.Enqueue(ctx, queueengine.Task{
    Queue: "default", Name: "jobs/example", Payload: []byte("hello"),
})
if err != nil { return err }
_ = id

lease, found, err := store.Claim(ctx, "default", 30*time.Second)
if err != nil { return err }
if found {
    // Only acknowledge after the actual job has succeeded.
    if err := store.Complete(ctx, lease.Task.ID, lease.Token); err != nil {
        return err
    }
}
```

The imports, `ctx`, and surrounding function are omitted for brevity.
The example is **not** a runnable command in the repository yet. No SQLite
driver dependency has been added to `go.mod`; integration will choose and
pin a driver before enabling standalone mode.

## Safety and behavior

- The SQLite file must be on a **local** filesystem with durable storage.
- `Open` enables WAL, FULL synchronous mode, and a busy timeout.
- One database connection serializes this store's access to SQLite.
- Claims atomically replace expired tokens; a stale worker cannot renew,
  complete, or fail a task after its lease expires or is superseded.
- A crash after a claim may cause re-delivery when its lease expires.
  This is **at-least-once delivery**, not exactly-once side effects.
- Lease duration must account for job runtime, or the worker must renew it.
- Claim uses SQLite `UPDATE ... RETURNING`, requiring SQLite 3.35 or newer.
- There is no distributed replication, automatic failover, or production
  backup orchestration yet.
- Do not point the new backend at a production path until testing and
  explicit deployment approval.

## Remaining work before standalone launch

1. Choose and pin a SQLite driver, integrate a standalone entrypoint.
2. Add tests for crash recovery, reopen persistence, concurrent claims,
   lease expiration, cancellation, and disk failures.
3. Connect worker execution and web dashboard to the backend interface.
4. Publish reproducible benchmarks and backup/restore guidance.
