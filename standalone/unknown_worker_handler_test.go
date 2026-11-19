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

// An unknown task name must never execute an unrelated registered handler.
// After rejecting the unknown task, the worker must still process valid work.
func TestUnknownHandlerRejectedAndValidJobContinues(t *testing.T) {
 ctx := context.Background()
 store, err := sqlite.Open(ctx, "sqlite3", filepath.Join(t.TempDir(), "unknown.db"))
 if err != nil { t.Fatal(err) }
 defer store.Close()

 var calls atomic.Int32
 var reg registry
 if err := reg.register("app/allowed", func(context.Context, []byte) error {
  calls.Add(1)
  return nil
 }); err != nil { t.Fatal(err) }

 for _, name := range []string{"app/not-registered", "app/allowed"} {
  if _, err := store.Enqueue(ctx, queueengine.Task{Queue: "default", Name: name}); err != nil {
   t.Fatal(err)
  }
 }

 w := &worker{store: store, lease: 3*time.Second, handlers: reg.snapshot(), maxAttempts: 1}
 for i := 0; i < 2; i++ {
  claim, found, err := store.Claim(ctx, "default", 3*time.Second)
  if err != nil || !found { t.Fatalf("claim %d: found=%v err=%v", i, found, err) }
  w.execute(ctx, claim)
 }
 if got := calls.Load(); got != 1 {
  t.Fatalf("allowed handler executions=%d, want 1", got)
 }
 if _, found, err := store.Claim(ctx, "default", time.Second); err != nil || found {
  t.Fatalf("unexpected remaining claim: found=%v err=%v", found, err)
 }
}
