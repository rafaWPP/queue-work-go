CREATE TABLE IF NOT EXISTS queue_tasks_completed (
    id INT AUTO_INCREMENT PRIMARY KEY,
    queue_task_id INT NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    data TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    exception TEXT,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
