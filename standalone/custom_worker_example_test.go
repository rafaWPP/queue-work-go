package main

import (
    "context"
    "path/filepath"
    "sync/atomic"
    "testing"
    "time"

    _ "github.com/mattn/go-sqlite3"
    "go-queue/internal/queueengine"
    "go-queue/internal/queueengine/sqlite"
)

// This example exercises a custom handler registered outside the built-in
// handler factory. Application code remains separate from the queue engine.
func TestCustomRegisteredHandlerExecutes(t *testing.T) {
    ctx := context.Background()
    store, err := sqlite.Open(ctx, "sqlite3", filepath.Join(t.TempDir(), "custom.db"))
    if err != nil { t.Fatal(err) }
    defer store.Close()

    var registry registry
    var executions atomic.Int32
    if err := registry.register("app/send-email", func(_ context.Context, payload []byte) error {
        if string(payload) != "customer:42" { t.Errorf("unexpected payload: %q", payload) }
        executions.Add(1)
        return nil
    }); err != nil { t.Fatal(err) }

    if _, err := store.Enqueue(ctx, queueengine.Task{
        Queue: "default", Name: "app/send-email", Payload: []byte("customer:42"),
    }); err != nil { t.Fatal(err) }

    claim, found, err := store.Claim(ctx, "default", 3*time.Second)
    if err != nil || !found { t.Fatalf("claim: found=%v err=%v", found, err) }

    w := &worker{store: store, lease: 3*time.Second, handlers: registry.snapshot()}
    w.execute(ctx, claim)
    if executions.Load() != 1 { t.Fatalf("executions=%d, want 1", executions.Load()) }
    _, found, err = store.Claim(ctx, "default", time.Second)
    if err != nil || found { t.Fatalf("completed job claimable: found=%v err=%v", found, err) }
}
