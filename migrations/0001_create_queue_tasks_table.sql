CREATE TABLE IF NOT EXISTS queue_tasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    queue_name VARCHAR(255) NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    data TEXT,
    status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
