// Package queueengine defines the persistence contract for future queue backends.
// The existing MariaDB worker is unchanged; standalone implementations must
// satisfy this contract before they can be selected by the application.
package queueengine

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("queue task not found")
	ErrNotOwner = errors.New("queue task lease is not owned by this worker")
)

type Task struct {
	ID string
	Queue string
	Name string
	Payload []byte
	AvailableAt time.Time
	Attempts int
}

type Lease struct {
	Task Task
	Token string
	ExpiresAt time.Time
}

// Store is a backend-neutral, durable task queue contract.
// Claim must atomically persist the ownership token before returning.
// Complete and Fail must reject expired or superseded ownership tokens.
// A backend must recover acknowledged durable tasks after process restart.
type Store interface {
	Enqueue(ctx context.Context, task Task) (string, error)
	Claim(ctx context.Context, queue string, leaseDuration time.Duration) (Lease, bool, error)
	Renew(ctx context.Context, taskID, token string, leaseDuration time.Duration) error
	Complete(ctx context.Context, taskID, token string) error
	Fail(ctx context.Context, taskID, token, reason string) error
	Close() error
}
