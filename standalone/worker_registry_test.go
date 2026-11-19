package main

import (
    "context"
    "testing"
)

func TestRegistryRejectsInvalidAndDuplicateHandlers(t *testing.T) {
    var r registry
    handler := func(context.Context, []byte) error { return nil }

    if err := r.register("", handler); err == nil {
        t.Fatal("empty handler name must be rejected")
    }
    if err := r.register("jobs/example", nil); err == nil {
        t.Fatal("nil handler must be rejected")
    }
    if err := r.register("jobs/example", handler); err != nil {
        t.Fatalf("register custom job: %v", err)
    }
    if err := r.register("jobs/example", handler); err == nil {
        t.Fatal("duplicate handler must be rejected")
    }
}

func TestRegistrySnapshotIsIndependent(t *testing.T) {
    var r registry
    handler := func(context.Context, []byte) error { return nil }
    if err := r.register("jobs/original", handler); err != nil {
        t.Fatal(err)
    }

    snapshot := r.snapshot()
    delete(snapshot, "jobs/original")
    snapshot["jobs/injected"] = handler

    fresh := r.snapshot()
    if _, ok := fresh["jobs/original"]; !ok {
        t.Fatal("snapshot mutation removed a registered job")
    }
    if _, ok := fresh["jobs/injected"]; ok {
        t.Fatal("snapshot mutation injected an unregistered job")
    }
}
