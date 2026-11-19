-- PREPARATORY MIGRATION ONLY. Do not run automatically.
-- Apply only after auditing existing queue_tasks schema and coordinating deployment.
-- A token is generated at claim time, checked by heartbeat, completion and archive,
-- and invalidated during recovery. Until all paths use it, this column alone
-- does NOT prevent stale workers from finishing a re-claimed task.
ALTER TABLE queue_tasks
  ADD COLUMN processing_token CHAR(36) NULL DEFAULT NULL;
CREATE INDEX idx_queue_tasks_processing_token
  ON queue_tasks (id, processing_token);
