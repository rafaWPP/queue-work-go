// A minimal, independent plugin for Queue Work Go.
// Build with: go build -buildmode=plugin -o examples/hello/main.so ./examples/hello
package main

import (
	"encoding/json"
	"fmt"

	"go-queue/common"
)

type helloJob struct {
	Message string `json:"message"`
}

func (j *helloJob) Handle() error {
	if j.Message == "" {
		return fmt.Errorf("message is required")
	}
	fmt.Printf("[hello] %s\n", j.Message)
	return nil
}

// New is the entrypoint expected by the queue worker.
func New(payload []byte) common.Job {
	var job helloJob
	if err := json.Unmarshal(payload, &job); err != nil {
		// Handle will return a clear validation error.
		return &invalidJob{err: err}
	}
	return &job
}

type invalidJob struct{ err error }

func (j *invalidJob) Handle() error { return fmt.Errorf("invalid JSON payload: %w", j.err) }
