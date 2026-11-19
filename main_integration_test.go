package main

import (
	"database/sql"
	"fmt"
	"strings"
	"os"
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
}
