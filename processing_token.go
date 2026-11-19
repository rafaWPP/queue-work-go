package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

// newProcessingToken creates a cryptographically random identifier for a single
// task claim. It must be stored atomically with the claim before being used for
// heartbeat, completion, or archival fencing.
func newProcessingToken() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate processing token: %w", err)
	}
	// RFC 4122 version 4 UUID, represented as a 36-character string.
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	var chars [36]byte
	hex.Encode(chars[0:8], bytes[0:4])
	chars[8] = '-'
	hex.Encode(chars[9:13], bytes[4:6])
	chars[13] = '-'
	hex.Encode(chars[14:18], bytes[6:8])
	chars[18] = '-'
	hex.Encode(chars[19:23], bytes[8:10])
	chars[23] = '-'
	hex.Encode(chars[24:36], bytes[10:16])
	return string(chars[:]), nil
}

// heartbeatWithToken refreshes the lease only while this execution owns the task.
// A zero-row update means the task was recovered, reassigned, or deleted.
func heartbeatWithToken(ctx context.Context, db *sql.DB, taskID int, token string) error {
	if token == "" {
		return fmt.Errorf("missing processing token for task %d", taskID)
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err := db.ExecContext(queryCtx,
		"UPDATE queue_tasks SET updated_at = NOW() WHERE id = ? AND status = 'processing' AND processing_token = ?",
		taskID, token)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}
	if affected != 0 {
		return fmt.Errorf("unexpected heartbeat row count for task %d: %d", taskID, affected)
	}
	// MariaDB reports changed rows, not matched rows. When a heartbeat runs
	// within the same timestamp second, updated_at may remain unchanged.
	// Verify ownership explicitly rather than treating that as a lost lease.
	var exists int
	err = db.QueryRowContext(queryCtx,
		"SELECT 1 FROM queue_tasks WHERE id = ? AND status = 'processing' AND processing_token = ?",
		taskID, token).Scan(&exists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: task %d", ErrNotOwner, taskID)
	}
	return err
}
