package main

import (
    "context"
    "log"
)

// defaultHandlers creates an immutable-by-convention snapshot of the worker's
// trusted built-in handlers. Job names received over HTTP are never executable
// commands: they must match one of the explicitly registered handlers.
func defaultHandlers() (map[string]Handler, error) {
    var r registry
    if err := r.register("builtin/noop", func(context.Context, []byte) error {
        return nil
    }); err != nil {
        return nil, err
    }
    if err := r.register("builtin/log", func(_ context.Context, payload []byte) error {
        log.Printf("processed log task (%d bytes)", len(payload))
        return nil
    }); err != nil {
        return nil, err
    }
    return r.snapshot(), nil
}
