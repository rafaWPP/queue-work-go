-- Standalone SQLite schema (design preview; not executed automatically).
-- This file is deliberately separate from MariaDB migrations.
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY NOT NULL,
    queue TEXT NOT NULL,
    name TEXT NOT NULL,
    payload BLOB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    available_at_ms INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    processing_token TEXT,
    lease_expires_at_ms INTEGER,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (
        (status = 'processing' AND processing_token IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
        OR
        (status <> 'processing' AND processing_token IS NULL AND lease_expires_at_ms IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_tasks_claim
ON tasks (queue, status, available_at_ms, id);

CREATE INDEX IF NOT EXISTS idx_tasks_expired_lease
ON tasks (status, lease_expires_at_ms)
WHERE status = 'processing';
