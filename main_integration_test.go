package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// This integration test requires a disposable database.
// Set QUEUE_TEST_DSN to run it; never point it at a production database.
func TestClaimNextTaskMariaDB(t *testing.T) {
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("set QUEUE_TEST_DSN for MariaDB integration tests")
	}
	// Fail closed unless the caller explicitly opts in to destructive tests.
	if os.Getenv("QUEUE_TEST_ALLOW_DESTRUCTIVE") != "yes" {
		t.Skip("set QUEUE_TEST_ALLOW_DESTRUCTIVE=yes only for a disposable test database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Validate the actual selected schema, not a potentially misleading DSN string.
	var schema string
	if err := db.QueryRow("SELECT DATABASE()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(strings.ToLower(schema), "queue_test") {
		t.Fatalf("refusing destructive integration test on database %q: require queue_test* schema", schema)
	}
	var existing int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'queue_tasks'").Scan(&existing); err != nil {
		t.Fatal(err)
	}
	if existing != 0 {
		t.Fatal("refusing to overwrite an existing queue_tasks table: use an empty disposable test database")
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}

	// Use a temporary table in this connection's isolated test database.
	// It is intentionally named queue_tasks because claimNextTask queries it.
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS queue_tasks (
		id INT AUTO_INCREMENT PRIMARY KEY,
		queue_name VARCHAR(255) NOT NULL,
		task_name VARCHAR(255) NOT NULL,
		data TEXT,
		status ENUM('pending','processing','completed','failed') DEFAULT 'pending',
		exception TEXT NULL,
		available_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS queue_tasks") })
	if _, err := db.Exec("DELETE FROM queue_tasks"); err != nil {
		t.Fatal(err)
	}

	const tasks = 24
	for i := 0; i < tasks; i++ {
		if _, err := db.Exec("INSERT INTO queue_tasks (queue_name, task_name, data, status) VALUES ('integration', 'example', ?, 'pending')", fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate old pending jobs; claiming must refresh the lease timestamp.
	if _, err := db.Exec("UPDATE queue_tasks SET updated_at = NOW() - INTERVAL 2 HOUR WHERE status = 'pending'"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids := make(chan int, tasks)
	errs := make(chan error, tasks)
	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// SKIP LOCKED may return no row while other transactions hold
			// locks, even though pending tasks remain. Retry in later rounds.
			for attempt := 0; attempt < tasks*4; attempt++ {
				id, _, _, found, err := claimNextTask(db, "integration")
				if err != nil {
					errs <- err
					return
				}
				if found {
					ids <- id
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("claim error: %v", err)
	}
	seen := map[int]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("task %d claimed twice", id)
		}
		seen[id] = true
	}
	if len(seen) != tasks {
		t.Errorf("claimed %d tasks, expected %d", len(seen), tasks)
	}
	var pending int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks WHERE status = 'pending'").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("%d tasks remain pending", pending)
	}
	var staleProcessing int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks WHERE status = 'processing' AND updated_at < NOW() - INTERVAL 1 MINUTE").Scan(&staleProcessing); err != nil {
		t.Fatal(err)
	}
	if staleProcessing != 0 {
		t.Errorf("%d freshly claimed tasks still have stale timestamps", staleProcessing)
	}
	// Archiving without a history table must fail without deleting the task.
	var sampleID int
	if err := db.QueryRow("SELECT id FROM queue_tasks WHERE status = 'processing' ORDER BY id LIMIT 1").Scan(&sampleID); err != nil {
		t.Fatal(err)
	}
	if err := archiveCompletedTask(db, sampleID, "integration", "example", "sample"); err == nil {
		t.Fatal("expected archive failure when history table is absent")
	}
	var status string
	if err := db.QueryRow("SELECT status FROM queue_tasks WHERE id = ?", sampleID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "processing" {
		t.Fatalf("archive failure changed task status to %q", status)
	}

	_, err = db.Exec(`CREATE TABLE queue_tasks_completed (
		id INT AUTO_INCREMENT PRIMARY KEY,
		queue_task_id INT NOT NULL,
		queue_name VARCHAR(255) NOT NULL,
		task_name VARCHAR(255) NOT NULL,
		data TEXT,
		status VARCHAR(32) NOT NULL,
		completed_at TIMESTAMP NOT NULL
	) ENGINE=InnoDB`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS queue_tasks_completed") })

	// A recovered task must not be archived or deleted by a stale worker.
	if _, err := db.Exec("UPDATE queue_tasks SET status = 'failed' WHERE id = ?", sampleID); err != nil {
		t.Fatal(err)
	}
	if err := archiveCompletedTask(db, sampleID, "integration", "example", "sample"); err == nil {
		t.Fatal("expected archive to reject recovered task")
	}
	var historyCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks_completed WHERE queue_task_id = ?", sampleID).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	if historyCount != 0 {
		t.Fatal("recovered task was incorrectly added to history")
	}
	if err := db.QueryRow("SELECT status FROM queue_tasks WHERE id = ?", sampleID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("recovered task unexpectedly changed to %q", status)
	}

	var activeID int
	if err := db.QueryRow("SELECT id FROM queue_tasks WHERE status = 'processing' ORDER BY id LIMIT 1").Scan(&activeID); err != nil {
		t.Fatal(err)
	}
	if err := archiveCompletedTask(db, activeID, "integration", "example", "sample"); err != nil {
		t.Fatalf("archive of active task failed: %v", err)
	}
	var remaining int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks WHERE id = ?", activeID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatal("archived task was not deleted")
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks_completed WHERE queue_task_id = ?", activeID).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	if historyCount != 1 {
		t.Fatalf("expected one history entry, got %d", historyCount)
	}
	// A live heartbeat must protect long-running jobs from stale recovery.
	var liveID int
	if err := db.QueryRow("SELECT id FROM queue_tasks WHERE status = 'processing' ORDER BY id LIMIT 1").Scan(&liveID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE queue_tasks SET updated_at = NOW() - INTERVAL 2 HOUR WHERE status = 'processing'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE queue_tasks SET updated_at = NOW() WHERE id = ? AND status = 'processing'", liveID); err != nil {
		t.Fatal(err)
	}
	recoverStuckProcessingTasks(db, 30*time.Minute)
	if err := db.QueryRow("SELECT status FROM queue_tasks WHERE id = ?", liveID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "processing" {
		t.Fatalf("live task incorrectly recovered as %q", status)
	}
	var recovered int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks WHERE status = 'failed'").Scan(&recovered); err != nil {
		t.Fatal(err)
	}
	if recovered < 2 {
		t.Fatalf("expected stale processing tasks to be recovered; found %d failed tasks", recovered)
	}
}
