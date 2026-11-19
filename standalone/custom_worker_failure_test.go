package main

import (
    "context"
    "errors"
    "path/filepath"
    "testing"
    "time"

    _ "github.com/mattn/go-sqlite3"
    "go-queue/internal/queueengine"
    "go-queue/internal/queueengine/sqlite"
)

func TestCustomHandlerFailureDoesNotRequeueWithoutRetries(t *testing.T) {
    ctx := context.Background()
    store, err := sqlite.Open(ctx, "sqlite3", filepath.Join(t.TempDir(), "failure.db"))
    if err != nil { t.Fatal(err) }
    defer store.Close()

    var reg registry
    if err := reg.register("app/failing", func(context.Context, []byte) error {
        return errors.New("intentional failure")
    }); err != nil { t.Fatal(err) }
    if _, err := store.Enqueue(ctx, queueengine.Task{Queue: "default", Name: "app/failing"}); err != nil {
        t.Fatal(err)
    }
    claim, found, err := store.Claim(ctx, "default", time.Second)
    if err != nil || !found { t.Fatalf("claim: found=%v err=%v", found, err) }
    (&worker{store: store, lease: time.Second, handlers: reg.snapshot(), maxAttempts: 1}).execute(ctx, claim)

    _, found, err = store.Claim(ctx, "default", time.Second)
    if err != nil || found { t.Fatalf("failed job claimable: found=%v err=%v", found, err) }
}

func TestCustomHandlerPanicIsIsolated(t *testing.T) {
    ctx := context.Background()
    store, err := sqlite.Open(ctx, "sqlite3", filepath.Join(t.TempDir(), "panic.db"))
    if err != nil { t.Fatal(err) }
    defer store.Close()

    var reg registry
    if err := reg.register("app/panicking", func(context.Context, []byte) error {
        panic("intentional test panic")
    }); err != nil { t.Fatal(err) }
    if _, err := store.Enqueue(ctx, queueengine.Task{Queue: "default", Name: "app/panicking"}); err != nil {
        t.Fatal(err)
    }
    claim, found, err := store.Claim(ctx, "default", time.Second)
    if err != nil || !found { t.Fatalf("claim: found=%v err=%v", found, err) }
    (&worker{store: store, lease: time.Second, handlers: reg.snapshot(), maxAttempts: 1}).execute(ctx, claim)

    _, found, err = store.Claim(ctx, "default", time.Second)
    if err != nil || found { t.Fatalf("panicked job claimable: found=%v err=%v", found, err) }
}
