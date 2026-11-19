package web

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ---------------------
// Tipos e Conexão
// ---------------------

// QueueTask representa um registro da tabela queue_tasks.
type QueueTask struct {
	ID          int       `json:"id"`
	QueueName   string    `json:"queue_name"`
	TaskName    string    `json:"task_name"`
	Data        string    `json:"data"`
	Status      string    `json:"status"`
	Exception   string    `json:"exception"`
	AvailableAt time.Time `json:"available_at"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type QueueTaskGroup struct {
	Label string      `json:"label"`
	Date  string      `json:"date"`
	Tasks []QueueTask `json:"tasks"`
}

type ActiveQueuesView struct {
	Processing      []QueueTask      `json:"processing"`
	PendingFuture   []QueueTaskGroup `json:"pending_future"`
	RecentFinalized []QueueTask      `json:"recent_finalized"`
}

type Pagination struct {
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalItems int `json:"total_items"`
	TotalPages int `json:"total_pages"`
}

type FailedTasksResponse struct {
	Tasks      []QueueTask `json:"tasks"`
	Pagination Pagination  `json:"pagination"`
}

type CompletedTasksResponse struct {
	Tasks      []CompletedTask `json:"tasks"`
	Pagination Pagination      `json:"pagination"`
}

// CompletedTask representa um registro da tabela queue_tasks_completed.
type CompletedTask struct {
	ID          int       `json:"id"`
	QueueTaskID int       `json:"queue_task_id"`
	QueueName   string    `json:"queue_name"`
	TaskName    string    `json:"task_name"`
	Data        string    `json:"data"`
	Status      string    `json:"status"`
	Exception   string    `json:"exception"`
	CompletedAt time.Time `json:"completed_at"`
}

type sendMessagePayload struct {
	SendMessageID int `json:"send_message_id"`
}

// getDSN monta a string de conexão utilizando variáveis de ambiente.
func getDSN() string {
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	tz := os.Getenv("APP_TIMEZONE")
	if tz == "" {
		tz = "Local"
	}
	loc := url.QueryEscape(tz)

	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=%s",
		user, pass, host, port, name, loc,
	)
}

// checkAuth verifica se o parâmetro "auth" da URL corresponde à chave definida no ambiente.
func checkAuth(w http.ResponseWriter, r *http.Request) bool {
	secret := os.Getenv("AUTH")
	if r.URL.Query().Get("auth") != secret {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func redirectBackToPage(w http.ResponseWriter, r *http.Request, fallbackPath string) {
	auth := r.URL.Query().Get("auth")
	redirectPath := r.URL.Query().Get("redirect")
	if redirectPath == "" {
		redirectPath = fallbackPath
	}
	http.Redirect(w, r, redirectPath+"?auth="+auth, http.StatusSeeOther)
}

// ---------------------
// Funções de Contagem
// ---------------------

func countActiveQueueTasks() (int, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	query := "SELECT COUNT(*) FROM queue_tasks WHERE status IN ('pending', 'processing')"
	err = db.QueryRow(query).Scan(&count)
	return count, err
}

func countFailedQueueTasks() (int, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	query := "SELECT COUNT(*) FROM queue_tasks WHERE status = 'failed'"
	err = db.QueryRow(query).Scan(&count)
	return count, err
}

func countCompletedQueueTasks() (int, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	query := "SELECT COUNT(*) FROM queue_tasks_completed"
	err = db.QueryRow(query).Scan(&count)
	return count, err
}

func getPaginationParams(r *http.Request, defaultPageSize int) (int, int) {
	page := 1
	pageSize := defaultPageSize

	if rawPage := r.URL.Query().Get("page"); rawPage != "" {
		if parsed, err := strconv.Atoi(rawPage); err == nil && parsed > 0 {
			page = parsed
		}
	}
	if rawPageSize := r.URL.Query().Get("page_size"); rawPageSize != "" {
		if parsed, err := strconv.Atoi(rawPageSize); err == nil && parsed > 0 && parsed <= 200 {
			pageSize = parsed
		}
	}

	return page, pageSize
}

func buildPagination(totalItems, page, pageSize int) Pagination {
	if pageSize <= 0 {
		pageSize = 50
	}
	totalPages := totalItems / pageSize
	if totalItems%pageSize != 0 {
		totalPages++
	}
	if totalPages == 0 {
		totalPages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}

	return Pagination{
		Page:       page,
		PageSize:   pageSize,
		TotalItems: totalItems,
		TotalPages: totalPages,
	}
}

// ---------------------
// Funções de Busca (Fetch)
// ---------------------

func fetchActiveQueueTasks() ([]QueueTask, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	query := "SELECT id, queue_name, task_name, data, status, available_at, created_at, updated_at FROM queue_tasks WHERE status IN ('pending', 'processing') ORDER BY status DESC, available_at ASC, id ASC"
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]QueueTask, 0)
	for rows.Next() {
		var t QueueTask
		var queueName, taskName, data, status sql.NullString
		var availableAt sql.NullTime
		if err := rows.Scan(&t.ID, &queueName, &taskName, &data, &status, &availableAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
			fmt.Printf("Erro ao escanear (active): %v\n", err)
			continue
		}
		if queueName.Valid {
			t.QueueName = queueName.String
		}
		if taskName.Valid {
			t.TaskName = taskName.String
		}
		if data.Valid {
			t.Data = data.String
		}
		if status.Valid {
			t.Status = status.String
		}
		if availableAt.Valid {
			t.AvailableAt = availableAt.Time
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func fetchRecentFinalizedQueueTasks(window time.Duration) ([]QueueTask, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	since := time.Now().Add(-window)
	recent := make([]QueueTask, 0)

	failedRows, err := db.Query(
		"SELECT id, queue_name, task_name, data, status, exception, available_at, created_at, updated_at FROM queue_tasks WHERE status = 'failed' AND updated_at >= ? ORDER BY updated_at DESC",
		since,
	)
	if err != nil {
		return nil, err
	}
	defer failedRows.Close()

	for failedRows.Next() {
		var t QueueTask
		var queueName, taskName, data, status, exception sql.NullString
		var availableAt sql.NullTime
		if err := failedRows.Scan(&t.ID, &queueName, &taskName, &data, &status, &exception, &availableAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
			continue
		}
		if queueName.Valid {
			t.QueueName = queueName.String
		}
		if taskName.Valid {
			t.TaskName = taskName.String
		}
		if data.Valid {
			t.Data = data.String
		}
		if status.Valid {
			t.Status = status.String
		}
		if exception.Valid {
			t.Exception = exception.String
		}
		if availableAt.Valid {
			t.AvailableAt = availableAt.Time
		}
		recent = append(recent, t)
	}

	completedRows, err := db.Query(
		`SELECT
			qtc.queue_task_id,
			qtc.queue_name,
			qtc.task_name,
			qtc.data,
			qtc.status,
			qtc.exception,
			qtc.completed_at
		FROM queue_tasks_completed qtc
		WHERE qtc.completed_at >= ?
		ORDER BY qtc.completed_at DESC`,
		since,
	)
	if err != nil {
		return nil, err
	}
	defer completedRows.Close()

	completedTasks := make([]QueueTask, 0)
	sendMessageIDs := make([]int, 0)
	sendMessageIDsByTask := make(map[int]int)

	for completedRows.Next() {
		var t QueueTask
		var queueName, taskName, data, status, exception sql.NullString
		var completedAt sql.NullTime
		if err := completedRows.Scan(&t.ID, &queueName, &taskName, &data, &status, &exception, &completedAt); err != nil {
			continue
		}
		if queueName.Valid {
			t.QueueName = queueName.String
		}
		if taskName.Valid {
			t.TaskName = taskName.String
		}
		if data.Valid {
			t.Data = data.String
		}
		if status.Valid {
			t.Status = status.String
		} else {
			t.Status = "completed"
		}
		if exception.Valid {
			t.Exception = exception.String
		}
		if completedAt.Valid {
			t.UpdatedAt = completedAt.Time
		}

		if t.Data != "" {
			var payload sendMessagePayload
			if err := json.Unmarshal([]byte(t.Data), &payload); err == nil && payload.SendMessageID > 0 {
				sendMessageIDs = append(sendMessageIDs, payload.SendMessageID)
				sendMessageIDsByTask[t.ID] = payload.SendMessageID
			}
		}

		completedTasks = append(completedTasks, t)
	}

	if len(sendMessageIDs) > 0 {
		placeholders := make([]string, len(sendMessageIDs))
		args := make([]interface{}, len(sendMessageIDs))
		for i, id := range sendMessageIDs {
			placeholders[i] = "?"
			args[i] = id
		}

		query := fmt.Sprintf("SELECT id, scheduled_at, created_at FROM send_messages_all WHERE id IN (%s)", strings.Join(placeholders, ","))
		metaRows, err := db.Query(query, args...)
		if err == nil {
			defer metaRows.Close()
			scheduleMap := make(map[int]struct {
				ScheduledAt sql.NullTime
				CreatedAt   sql.NullTime
			})

			for metaRows.Next() {
				var id int
				var scheduledAt, createdAt sql.NullTime
				if err := metaRows.Scan(&id, &scheduledAt, &createdAt); err != nil {
					continue
				}
				scheduleMap[id] = struct {
					ScheduledAt sql.NullTime
					CreatedAt   sql.NullTime
				}{ScheduledAt: scheduledAt, CreatedAt: createdAt}
			}

			for i := range completedTasks {
				sendMessageID, ok := sendMessageIDsByTask[completedTasks[i].ID]
				if !ok {
					continue
				}
				meta, ok := scheduleMap[sendMessageID]
				if !ok {
					continue
				}
				if meta.ScheduledAt.Valid {
					completedTasks[i].AvailableAt = meta.ScheduledAt.Time
				}
				if meta.CreatedAt.Valid {
					completedTasks[i].CreatedAt = meta.CreatedAt.Time
				}
			}
		}
		}

		recent = append(recent, completedTasks...)

	sort.Slice(recent, func(i, j int) bool {
		return recent[i].UpdatedAt.After(recent[j].UpdatedAt)
	})

	return recent, nil
}

func groupActiveQueueTasks(tasks []QueueTask) ActiveQueuesView {
	view := ActiveQueuesView{
		Processing:      make([]QueueTask, 0),
		PendingFuture:   make([]QueueTaskGroup, 0),
		RecentFinalized: make([]QueueTask, 0),
	}

	now := time.Now()
	today := now.Format("2006-01-02")
	groupMap := make(map[string]*QueueTaskGroup)
	groupOrder := make([]string, 0)

	for _, task := range tasks {
		if task.Status == "processing" {
			view.Processing = append(view.Processing, task)
			continue
		}
		if task.Status != "pending" {
			continue
		}

		groupDate := "Sem agendamento"
		label := "Sem agendamento"
		if !task.AvailableAt.IsZero() {
			groupDate = task.AvailableAt.Format("2006-01-02")
			label = task.AvailableAt.Format("02/01/2006")
			switch {
			case groupDate == today:
				label = "Hoje mais tarde - " + label
			case groupDate == now.AddDate(0, 0, 1).Format("2006-01-02"):
				label = "Amanhã - " + label
			}
		}

		if _, ok := groupMap[groupDate]; !ok {
			groupMap[groupDate] = &QueueTaskGroup{
				Label: label,
				Date:  groupDate,
				Tasks: make([]QueueTask, 0),
			}
			groupOrder = append(groupOrder, groupDate)
		}
		groupMap[groupDate].Tasks = append(groupMap[groupDate].Tasks, task)
	}

	sort.Strings(groupOrder)
	for _, dateKey := range groupOrder {
		group := *groupMap[dateKey]
		view.PendingFuture = append(view.PendingFuture, group)
	}

	return view
}

func fetchFailedQueueTasks(page, pageSize int) ([]QueueTask, Pagination, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, Pagination{}, err
	}
	defer db.Close()

	var totalItems int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks WHERE status = 'failed'").Scan(&totalItems); err != nil {
		return nil, Pagination{}, err
	}
	pagination := buildPagination(totalItems, page, pageSize)
	offset := (pagination.Page - 1) * pagination.PageSize

	query := "SELECT id, queue_name, task_name, data, status, exception, created_at, updated_at FROM queue_tasks WHERE status = 'failed' ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?"
	rows, err := db.Query(query, pagination.PageSize, offset)
	if err != nil {
		return nil, Pagination{}, err
	}
	defer rows.Close()

	tasks := make([]QueueTask, 0)
	for rows.Next() {
		var t QueueTask
		var queueName, taskName, data, status, exception sql.NullString
		if err := rows.Scan(&t.ID, &queueName, &taskName, &data, &status, &exception, &t.CreatedAt, &t.UpdatedAt); err != nil {
			fmt.Printf("Erro ao escanear (failed): %v\n", err)
			continue
		}
		if queueName.Valid {
			t.QueueName = queueName.String
		}
		if taskName.Valid {
			t.TaskName = taskName.String
		}
		if data.Valid {
			t.Data = data.String
		}
		if status.Valid {
			t.Status = status.String
		}
		if exception.Valid {
			t.Exception = exception.String
		}
		tasks = append(tasks, t)
	}
	return tasks, pagination, nil
}

func fetchCompletedTasks(page, pageSize int) ([]CompletedTask, Pagination, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, Pagination{}, err
	}
	defer db.Close()

	var totalItems int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_tasks_completed").Scan(&totalItems); err != nil {
		return nil, Pagination{}, err
	}
	pagination := buildPagination(totalItems, page, pageSize)
	offset := (pagination.Page - 1) * pagination.PageSize

	query := "SELECT id, queue_task_id, queue_name, task_name, data, status, exception, completed_at FROM queue_tasks_completed ORDER BY completed_at DESC, id DESC LIMIT ? OFFSET ?"
	rows, err := db.Query(query, pagination.PageSize, offset)
	if err != nil {
		return nil, Pagination{}, err
	}
	defer rows.Close()
	tasks := make([]CompletedTask, 0)
	for rows.Next() {
		var t CompletedTask
		var queueName, taskName, data, status, exception sql.NullString
		var completedAt sql.NullTime
		if err := rows.Scan(&t.ID, &t.QueueTaskID, &queueName, &taskName, &data, &status, &exception, &completedAt); err != nil {
			fmt.Printf("Erro ao escanear (completed): %v\n", err)
			continue
		}
		if queueName.Valid {
			t.QueueName = queueName.String
		}
		if taskName.Valid {
			t.TaskName = taskName.String
		}
		if data.Valid {
			t.Data = data.String
		}
		if status.Valid {
			t.Status = status.String
		}
		if exception.Valid {
			t.Exception = exception.String
		}
		if completedAt.Valid {
			t.CompletedAt = completedAt.Time
		}
		tasks = append(tasks, t)
	}
	return tasks, pagination, nil
}

// ---------------------
// Handlers de Limpeza
// ---------------------

func ClearActiveHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		http.Error(w, "Erro ao conectar ao banco", http.StatusInternalServerError)
		return
	}
	defer db.Close()
	query := "DELETE FROM queue_tasks WHERE status IN ('pending', 'processing')"
	_, err = db.Exec(query)
	if err != nil {
		http.Error(w, "Erro ao limpar filas ativas", http.StatusInternalServerError)
		return
	}
	redirectBackToPage(w, r, "/")
}

func ClearFailedHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		http.Error(w, "Erro ao conectar ao banco", http.StatusInternalServerError)
		return
	}
	defer db.Close()
	query := "DELETE FROM queue_tasks WHERE status = 'failed'"
	_, err = db.Exec(query)
	if err != nil {
		http.Error(w, "Erro ao limpar filas com falha", http.StatusInternalServerError)
		return
	}
	redirectBackToPage(w, r, "/")
}

func ClearCompletedHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		http.Error(w, "Erro ao conectar ao banco", http.StatusInternalServerError)
		return
	}
	defer db.Close()
	query := "DELETE FROM queue_tasks_completed"
	_, err = db.Exec(query)
	if err != nil {
		http.Error(w, "Erro ao limpar filas completadas", http.StatusInternalServerError)
		return
	}
	redirectBackToPage(w, r, "/")
}

// ---------------------
// Templates
// ---------------------

// Template base sem meta refresh para que não recarregue o navegador.
const baseTemplate = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{.Title}}</title>
  <link rel="icon" type="image/png" href="/queue-icon.png">
  <style>
    :root {
      --topbar: #181818;
      --bg: #212121;
      --sidebar: #202020;
      --panel: #262626;
      --panel-soft: #2b2b2b;
      --line: rgba(255, 255, 255, 0.08);
      --text: #f5f7fb;
      --muted: #b3bac7;
      --accent: #6c7aff;
      --accent-soft: rgba(108, 122, 255, 0.14);
      --success: #17c964;
      --danger: #ff5d73;
      --warning: #ffb84d;
      --shadow: 0 8px 18px rgba(0, 0, 0, 0.14);
      --radius: 14px;
    }
    * {
      box-sizing: border-box;
    }
    body {
      font-family: "Segoe UI", "Helvetica Neue", sans-serif;
      background: var(--bg);
      margin: 0;
      padding: 0;
      color: var(--text);
      min-height: 100vh;
    }
    a {
      color: inherit;
    }
    .topbar {
      position: sticky;
      top: 0;
      z-index: 10;
      display: flex;
      align-items: center;
      justify-content: flex-start;
      padding: 1.1rem 1.5rem;
      background: var(--topbar);
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
      backdrop-filter: blur(14px);
    }
    .topbar-logo {
      display: inline-flex;
      align-items: center;
    }
    .topbar-logo img {
      width: 128px;
      max-width: min(38vw, 128px);
      object-fit: contain;
      filter: drop-shadow(0 2px 6px rgba(0, 0, 0, 0.18));
    }
    .app-shell {
      display: flex;
      gap: 1.75rem;
      max-width: 1480px;
      margin: 0 auto;
      padding: 1.6rem;
    }
    .sidebar {
      width: 260px;
      flex: 0 0 260px;
    }
    .sidebar-card {
      position: sticky;
      top: 94px;
      background: var(--sidebar);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: var(--shadow);
      padding: 1.2rem;
    }
    .sidebar-head {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.14em;
      font-size: 0.72rem;
      margin: 0 0 1rem;
    }
    .nav-links {
      display: grid;
      gap: 0.55rem;
    }
    .nav-links a,
    .nav-links button {
      display: flex;
      align-items: center;
      width: 100%;
      appearance: none;
      border: 1px solid transparent;
      background: transparent;
      color: var(--muted);
      border-radius: 10px;
      padding: 0.95rem 1rem;
      text-decoration: none;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.18s ease;
    }
    .nav-links a:hover,
    .nav-links button:hover {
      background: rgba(255, 255, 255, 0.05);
      border-color: rgba(255, 255, 255, 0.06);
      color: #fff;
    }
    .nav-links a.active {
      background: rgba(111, 124, 255, 0.16);
      color: #fff;
      border-color: rgba(111, 124, 255, 0.24);
    }
    .main-content {
      min-width: 0;
      flex: 1;
    }
    .container {
      margin: 0;
      padding: 0;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 1.6rem;
      margin-bottom: 1.5rem;
    }
    .tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 0.6rem;
      margin: 1rem 0 1.35rem;
    }
    .tab-button {
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 999px;
      color: var(--muted);
      cursor: pointer;
      font-weight: 600;
      padding: 0.58rem 1rem;
    }
    .tab-button.active {
      background: rgba(111, 124, 255, 0.18);
      border-color: rgba(111, 124, 255, 0.36);
      color: #fff;
    }
    .tab-panel {
      display: none;
      margin-top: 1rem;
    }
    .tab-panel.active {
      display: block;
    }
    h1 {
      font-size: clamp(1.8rem, 2.2vw, 2.6rem);
      margin: 0 0 1rem;
      color: #f8fbff;
      letter-spacing: -0.03em;
    }
    h2 {
      color: #f8fbff;
      margin: 0;
      font-size: 1.25rem;
    }
    h3 {
      color: var(--muted);
      margin: 0 0 0.75rem;
      font-size: 0.92rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    p {
      color: var(--muted);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      background: rgba(255, 255, 255, 0.018);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 12px;
      overflow: hidden;
    }
    th, td {
      padding: 0.75rem;
      text-align: left;
      border-bottom: 1px solid rgba(148, 163, 184, 0.08);
    }
    th {
      background: rgba(255, 255, 255, 0.05);
      color: #f8fbff;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    tr:nth-child(even) {
      background: rgba(255, 255, 255, 0.015);
    }
    td {
      color: #dbe7ff;
      vertical-align: top;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
    }
    .hero-card {
      position: relative;
      overflow: hidden;
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(240px, 0.75fr);
      gap: 1.4rem;
      align-items: center;
      min-height: 250px;
    }
    .hero-card::before {
      display: none;
    }
    .hero-content,
    .hero-art {
      position: relative;
      z-index: 1;
    }
    .hero-kicker {
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      padding: 0.42rem 0.78rem;
      border-radius: 999px;
      background: var(--accent-soft);
      color: #d8d0ff;
      font-size: 0.74rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 1rem;
    }
    .hero-card h1 {
      margin-bottom: 0.8rem;
    }
    .hero-copy {
      max-width: 640px;
      font-size: 1rem;
      line-height: 1.7;
    }
    .hero-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      margin-top: 1.35rem;
    }
    .hero-logo-panel {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 180px;
      border-radius: 12px;
      background: var(--panel-soft);
      border: 1px solid rgba(255, 255, 255, 0.08);
    }
    .hero-logo-panel img {
      width: min(100%, 250px);
      object-fit: contain;
    }
    .stat-card {
      background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 12px;
      padding: 1.25rem;
    }
    .stat-label {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 0.76rem;
      margin-bottom: 0.85rem;
    }
    .stat-value {
      color: #fff;
      font-size: 2.1rem;
      font-weight: 700;
      letter-spacing: -0.04em;
      margin-bottom: 1rem;
    }
    .stat-actions {
      display: flex;
      gap: 0.75rem;
      flex-wrap: wrap;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      padding: 0.35rem 0.7rem;
      border-radius: 999px;
      background: rgba(111, 124, 255, 0.12);
      border: 1px solid rgba(111, 124, 255, 0.18);
      color: #e2ddff;
      font-size: 0.78rem;
      font-weight: 600;
    }
    .button-primary,
    .button-secondary,
    .button-danger {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      text-decoration: none;
      border-radius: 10px;
      padding: 0.82rem 1.05rem;
      font-weight: 700;
      transition: all 0.18s ease;
      border: 1px solid transparent;
    }
    .button-primary {
      background: var(--accent);
      color: #fff;
      box-shadow: 0 10px 20px rgba(111, 124, 255, 0.18);
    }
    .button-primary:hover {
      transform: translateY(-1px);
      filter: brightness(1.04);
    }
    .button-secondary {
      background: rgba(255, 255, 255, 0.04);
      border-color: rgba(255, 255, 255, 0.08);
      color: var(--text);
    }
    .button-secondary:hover {
      transform: translateY(-1px);
      border-color: rgba(255, 255, 255, 0.14);
      background: rgba(255, 255, 255, 0.06);
    }
    .button-danger {
      background: rgba(255, 93, 115, 0.14);
      border-color: rgba(255, 93, 115, 0.26);
      color: #ffc4cd;
    }
    .button-danger:hover {
      transform: translateY(-1px);
      background: rgba(255, 93, 115, 0.2);
      border-color: rgba(255, 93, 115, 0.36);
    }
    .toolbar-note {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
      margin: 0 0 1.25rem;
      flex-wrap: wrap;
    }
    .section-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
      margin-bottom: 1.15rem;
      flex-wrap: wrap;
    }
    .meta-text {
      color: var(--muted);
      font-size: 0.92rem;
    }
    .status-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 92px;
      padding: 0.38rem 0.68rem;
      border-radius: 999px;
      border: 1px solid transparent;
      font-size: 0.74rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .status-badge.pending {
      background: rgba(255, 184, 77, 0.14);
      border-color: rgba(255, 184, 77, 0.22);
      color: #ffd48c;
    }
    .status-badge.processing {
      background: rgba(111, 124, 255, 0.16);
      border-color: rgba(111, 124, 255, 0.24);
      color: #c9d0ff;
    }
    .status-badge.failed,
    .status-badge.error {
      background: rgba(255, 93, 115, 0.16);
      border-color: rgba(255, 93, 115, 0.24);
      color: #ffb0bc;
    }
    .status-badge.completed,
    .status-badge.sent {
      background: rgba(23, 201, 100, 0.16);
      border-color: rgba(23, 201, 100, 0.24);
      color: #8df0b7;
    }
    .badge-countdown {
      margin-left: 0.3rem;
      opacity: 0.86;
      font-weight: 800;
    }
    .pager {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      margin-top: 1rem;
      flex-wrap: wrap;
    }
    .pager-controls {
      display: flex;
      align-items: center;
      gap: 0.55rem;
      flex-wrap: wrap;
    }
    .pager button {
      appearance: none;
      border: 1px solid rgba(255, 255, 255, 0.08);
      background: rgba(255, 255, 255, 0.04);
      color: var(--text);
      border-radius: 10px;
      padding: 0.62rem 0.92rem;
      font-weight: 600;
      cursor: pointer;
    }
    .pager button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }
    .pager-info {
      color: var(--muted);
      font-size: 0.9rem;
    }
    .exception-modal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.68);
      justify-content: center;
      align-items: center;
      padding: 1.2rem;
      z-index: 30;
    }
    .exception-panel {
      width: min(760px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 1.2rem;
    }
    .exception-panel pre {
      white-space: pre-wrap;
      max-height: 420px;
      overflow: auto;
      background: #1f1f1f;
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 10px;
      padding: 1rem;
      color: #e8edf8;
    }
    .confirm-modal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.72);
      justify-content: center;
      align-items: center;
      padding: 1.2rem;
      z-index: 40;
    }
    .confirm-panel {
      width: min(460px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 1.25rem;
    }
    .confirm-panel h3 {
      color: #fff;
      margin-bottom: 0.6rem;
    }
    .confirm-actions {
      display: flex;
      justify-content: flex-end;
      gap: 0.7rem;
      margin-top: 1.2rem;
      flex-wrap: wrap;
    }
    .icon-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 36px;
      height: 36px;
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.04);
      color: #e7ebff;
      cursor: pointer;
      transition: all 0.18s ease;
    }
    .icon-button:hover {
      background: rgba(108, 122, 255, 0.16);
      border-color: rgba(108, 122, 255, 0.24);
    }
    .icon-button svg {
      width: 17px;
      height: 17px;
      display: block;
    }
    .table-shell {
      min-height: 420px;
      display: flex;
      flex-direction: column;
      justify-content: flex-start;
    }
    .processing-shell {
      min-height: 220px;
      max-height: 220px;
      overflow-y: auto;
      padding-right: 0.2rem;
      margin-bottom: 1.75rem;
      border: 1px dashed rgba(255,255,255,0.08);
      border-radius: 12px;
      background: rgba(255,255,255,0.015);
      padding: 0.85rem;
    }
    .processing-shell.is-empty {
      overflow: hidden;
      padding-right: 0.85rem;
    }
    .processing-shell.is-empty .empty-state {
      min-height: 180px;
      height: 180px;
      padding: 1.25rem 1rem;
      border: 0;
      background: transparent;
    }
    .processing-shell table {
      margin: 0;
      border: 0;
      border-radius: 0;
      background: transparent;
    }
    .processing-shell thead th:first-child {
      border-top-left-radius: 8px;
    }
    .processing-shell thead th:last-child {
      border-top-right-radius: 8px;
    }
    .loading-state,
    .empty-state {
      min-height: 320px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 0.9rem;
      text-align: center;
      color: var(--muted);
      border: 1px dashed rgba(255,255,255,0.08);
      border-radius: 12px;
      background: rgba(255,255,255,0.015);
      padding: 2rem 1.25rem;
    }
    .spinner {
      width: 34px;
      height: 34px;
      border-radius: 999px;
      border: 3px solid rgba(255,255,255,0.08);
      border-top-color: var(--accent);
      animation: spin 0.85s linear infinite;
    }
    .empty-icon {
      width: 52px;
      height: 52px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 14px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.08);
    }
    .empty-icon svg {
      width: 26px;
      height: 26px;
      opacity: 0.82;
    }
    .details-modal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.78);
      justify-content: center;
      align-items: flex-start;
      padding: 2rem 1.2rem;
      z-index: 46;
      overflow-y: auto;
    }
    .details-panel {
      width: min(980px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 1rem;
    }
    .details-section {
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      overflow: hidden;
      background: rgba(255,255,255,0.015);
      margin-bottom: 1rem;
    }
    .details-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      padding: 1rem 1.1rem;
      background: #3a4253;
      color: #fff;
      font-weight: 700;
    }
    .details-toggle {
      appearance: none;
      border: none;
      background: transparent;
      color: #b78cff;
      font-weight: 700;
      cursor: pointer;
    }
    .details-body {
      padding: 1.1rem;
    }
    .details-grid {
      display: grid;
      grid-template-columns: 180px 1fr;
      gap: 0.8rem 1.2rem;
    }
    .details-label {
      color: var(--muted);
    }
    .details-value {
      color: #fff;
      word-break: break-word;
    }
    .details-json {
      margin: 0;
      white-space: pre-wrap;
      max-height: 460px;
      overflow: auto;
      background: #262b39;
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 10px;
      padding: 1rem;
      color: #eef2ff;
    }
    .queue-link {
      color: #c8d0ff;
      text-decoration: none;
      font-weight: 600;
      cursor: pointer;
    }
    .queue-link:hover {
      color: #fff;
      text-decoration: underline;
    }
    @keyframes spin {
      to {
        transform: rotate(360deg);
      }
    }
    .toolbar-note p {
      margin: 0;
    }
    @media (max-width: 900px) {
      .app-shell {
        flex-direction: column;
      }
      .sidebar {
        width: 100%;
        flex-basis: auto;
      }
      .sidebar-card {
        position: static;
      }
      .hero-card {
        grid-template-columns: 1fr;
      }
      .hero-logo-panel {
        min-height: 150px;
      }
      .details-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="topbar">
    <a class="topbar-logo" href="/?auth={{.AuthKey}}">
      <img src="/logo-queu.png" alt="WebTech Queues">
    </a>
  </div>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="sidebar-card">
        <div class="sidebar-head">Navegação</div>
        <div class="nav-links">
          <a href="/?auth={{.AuthKey}}" class="{{if eq .Page "home"}}active{{end}}">Dashboard</a>
          <a href="/queues?auth={{.AuthKey}}" class="{{if eq .Page "queues"}}active{{end}}">Agendados</a>
          <a href="/queues_failed?auth={{.AuthKey}}" class="{{if eq .Page "failed"}}active{{end}}">Falhas</a>
          <a href="/queues_completed?auth={{.AuthKey}}" class="{{if eq .Page "completed"}}active{{end}}">Concluídas</a>
          <button onclick="location.reload()">Atualizar</button>
        </div>
      </div>
    </aside>
    <main class="main-content">
      <div class="container">
        <div class="card">
          {{template "content" .}}
        </div>
      </div>
    </main>
  </div>
  <div id="confirmModal" class="confirm-modal">
    <div class="confirm-panel">
      <h3>Confirmar limpeza</h3>
      <p id="confirmModalText">Deseja continuar?</p>
      <div class="confirm-actions">
        <button class="button-secondary" type="button" onclick="closeConfirmModal()">Cancelar</button>
        <a id="confirmModalAction" class="button-danger" href="#">Confirmar</a>
      </div>
    </div>
  </div>
  <div id="detailsModal" class="details-modal">
    <div class="details-panel">
      <div class="details-section">
        <div class="details-head">
          <span id="detailsTitle">Detalhes da queue</span>
          <button class="details-toggle" type="button" onclick="closeDetailsModal()">Fechar</button>
        </div>
        <div class="details-body">
          <div class="details-grid" id="detailsMeta"></div>
        </div>
      </div>
      <div class="details-section">
        <div class="details-head">
          <span>Data</span>
          <button class="details-toggle" type="button" onclick="toggleDetailsSection('detailsDataBody', this)">Collapse</button>
        </div>
        <div class="details-body" id="detailsDataBody">
          <pre class="details-json" id="detailsData"></pre>
        </div>
      </div>
    </div>
  </div>
  <script>
    function openConfirmModal(href, message) {
      document.getElementById("confirmModalText").textContent = message || "Deseja continuar?";
      document.getElementById("confirmModalAction").setAttribute("href", href);
      document.getElementById("confirmModal").style.display = "flex";
    }

    function closeConfirmModal() {
      document.getElementById("confirmModal").style.display = "none";
    }

    function baseEscapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function formatTaskData(raw) {
      if (!raw) return "-";
      try {
        return JSON.stringify(JSON.parse(raw), null, 2);
      } catch (error) {
        return String(raw);
      }
    }

    function openDetailsModal(encodedTask) {
      let task = {};
      try {
        task = JSON.parse(decodeURIComponent(escape(atob(encodedTask))));
      } catch (error) {
        console.error(error);
        return;
      }

      document.getElementById("detailsTitle").textContent = task.task_name || task.queue_name || "Detalhes da queue";
      const meta = [
        ["ID", task.id ?? "-"],
        ["Queue", task.queue_name ?? "-"],
        ["Task", task.task_name ?? "-"],
        ["Status", task.status ?? "-"],
        ["Created", task.created_at ?? "-"],
        ["Available At", task.available_at ?? "-"],
        ["Updated", task.updated_at ?? "-"],
        ["Completed", task.completed_at ?? "-"],
        ["Queue Task ID", task.queue_task_id ?? "-"],
      ];

      document.getElementById("detailsMeta").innerHTML = meta
        .filter(item => item[1] && item[1] !== "-")
        .map(item => '<div class="details-label">' + baseEscapeHtml(item[0]) + '</div><div class="details-value">' + baseEscapeHtml(item[1]) + '</div>')
        .join("");

      document.getElementById("detailsData").textContent = formatTaskData(task.data);
      document.getElementById("detailsDataBody").style.display = "block";
      document.getElementById("detailsModal").style.display = "flex";
    }

    function closeDetailsModal() {
      document.getElementById("detailsModal").style.display = "none";
    }

    function toggleDetailsSection(id, button) {
      const body = document.getElementById(id);
      if (!body) return;
      const isHidden = body.style.display === "none";
      body.style.display = isHidden ? "block" : "none";
      button.textContent = isHidden ? "Collapse" : "Expand";
    }

    function encodeTaskPayload(task) {
      return btoa(unescape(encodeURIComponent(JSON.stringify(task))));
    }

    document.addEventListener("click", function (event) {
      const queueLink = event.target.closest(".queue-link[data-task]");
      if (queueLink) {
        event.preventDefault();
        openDetailsModal(queueLink.getAttribute("data-task"));
        return;
      }

      const trigger = event.target.closest("[data-confirm-action]");
      if (!trigger) return;
      event.preventDefault();
      openConfirmModal(trigger.getAttribute("href"), trigger.getAttribute("data-confirm-message"));
    });
  </script>
</body>
</html>
`

// Home template com 3 cards e atualização automática dos números via AJAX.
const homeTemplateCards = `
{{define "content"}}
  <div class="hero-card card" style="margin:-0.2rem 0 1.5rem;">
    <div class="hero-content">
      <div class="hero-kicker">Queue Control</div>
      <h1>WebTech Queues</h1>
      <p class="hero-copy">Fila, monitoramento e recuperação. Um painel operacional para acompanhar agendamentos, falhas, histórico e estabilidade do worker em um fluxo mais claro.</p>
      <div class="hero-actions">
        <a class="button-primary" href="/queues?auth={{.AuthKey}}">Abrir agendados</a>
        <a class="button-secondary" href="/queues_failed?auth={{.AuthKey}}">Ver falhas</a>
      </div>
    </div>
    <div class="hero-art">
      <div class="hero-logo-panel">
        <img src="/logo-queu.png" alt="WebTech Queues">
      </div>
    </div>
  </div>
  <div class="stat-grid">
    <div class="stat-card">
      <div class="stat-label">Filas Ativas</div>
      <div class="stat-value" id="activeCount">{{.ActiveCount}}</div>
      <div class="stat-actions">
        <a class="button-secondary" href="/queues?auth={{.AuthKey}}">Abrir fila</a>
        <a class="button-danger" href="/clear/active?auth={{.AuthKey}}&redirect=/" data-confirm-action="true" data-confirm-message="Limpar todas as tasks ativas e agendadas?">Limpar</a>
      </div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Falhas</div>
      <div class="stat-value" id="failedCount">{{.FailedCount}}</div>
      <div class="stat-actions">
        <a class="button-secondary" href="/queues_failed?auth={{.AuthKey}}">Ver falhas</a>
        <a class="button-danger" href="/clear/failed?auth={{.AuthKey}}&redirect=/" data-confirm-action="true" data-confirm-message="Limpar todas as tasks com falha?">Limpar</a>
      </div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Concluídas</div>
      <div class="stat-value" id="completedCount">{{.CompletedCount}}</div>
      <div class="stat-actions">
        <a class="button-secondary" href="/queues_completed?auth={{.AuthKey}}">Ver histórico</a>
        <a class="button-danger" href="/clear/completed?auth={{.AuthKey}}&redirect=/" data-confirm-action="true" data-confirm-message="Limpar todo o histórico de concluídas?">Limpar</a>
      </div>
    </div>
  </div>
  <script>
    function updateCounts() {
      fetch('/api/counts?auth={{.AuthKey}}')
        .then(response => response.json())
        .then(data => {
          document.getElementById("activeCount").textContent = data.active;
          document.getElementById("failedCount").textContent = data.failed;
          document.getElementById("completedCount").textContent = data.completed;
        })
        .catch(err => console.error(err));
    }
    setInterval(updateCounts, 5000);
    updateCounts();
  </script>
{{end}}
`

// Template para a tabela de filas ativas (pending/processing) com atualização via AJAX.
const queuesTemplate = `
{{define "content"}}
  <div class="section-head">
    <div><h1>Tasks Agendadas</h1></div>
    <a class="button-danger" href="/clear/active?auth={{.AuthKey}}&redirect=/queues" data-confirm-action="true" data-confirm-message="Limpar todas as tasks ativas e agendadas?">Limpar</a>
  </div>
  <div id="activeQueuesContent" class="table-shell"></div>
  <script>
    let activeQueueTab = "";
    const queuePageSize = 20;
    const queueGroupPages = {};
    let activeQueuesFirstLoad = true;
    const processingHoldSeconds = 10;
    const recentProcessingState = {};
    let latestActiveQueuesData = null;

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function renderStatusBadge(status, countdownSeconds) {
      const raw = String(status || "").trim();
      const normalized = raw.toLowerCase().replace(/[^a-z]/g, "");
      const countdown = typeof countdownSeconds === "number" && countdownSeconds >= 0
        ? ' <span class="badge-countdown">' + countdownSeconds + 's</span>'
        : "";
      return '<span class="status-badge ' + escapeHtml(normalized || "pending") + '">' + escapeHtml(raw || "-") + countdown + '</span>';
    }

    function renderAvailableAtBadge(value) {
      if (!value || String(value).startsWith("0001-01-01")) {
        return '<span class="status-badge pending">Sem agendamento</span>';
      }
      return new Date(value).toLocaleString();
    }

    function renderDateTime(value, emptyLabel) {
      if (!value || String(value).startsWith("0001-01-01")) {
        return emptyLabel || "-";
      }
      return new Date(value).toLocaleString();
    }

    function renderTaskTable(tasks, emptyLabel, options) {
      const statusCountdowns = (options && options.statusCountdowns) || {};

      let rows = "<table><thead><tr>";
      rows += "<th>ID</th>";
      rows += "<th>Queue Name</th>";
      rows += "<th>Task Name</th>";
      rows += "<th>Status</th>";
      rows += "<th>Available At (Agendado)</th>";
      rows += "<th>Created At</th>";
      rows += "</tr></thead><tbody>";

      if (!tasks || tasks.length === 0) {
        rows += "<tr><td colspan='6'>" + renderEmptyState(emptyLabel) + "</td></tr>";
        rows += "</tbody></table>";
        return rows;
      }

      tasks.forEach(task => {
        const encodedTask = encodeTaskPayload(task);
        rows += "<tr>";
        rows += "<td>" + task.id + "</td>";
        rows += "<td><a href='#' class='queue-link' data-task='" + escapeHtml(encodedTask) + "'>" + escapeHtml(task.queue_name) + "</a></td>";
        rows += "<td>" + escapeHtml(task.task_name) + "</td>";
        rows += "<td>" + renderStatusBadge(task.status, statusCountdowns[task.id]) + "</td>";
        rows += "<td>" + renderAvailableAtBadge(task.available_at) + "</td>";
        rows += "<td>" + renderDateTime(task.created_at, "-") + "</td>";
        rows += "</tr>";
      });

      rows += "</tbody></table>";
      return rows;
    }

    function normalizeRecentStatus(status) {
      const raw = String(status || "").toLowerCase();
      if (raw === "failed" || raw === "error") return "failed";
      if (raw === "completed" || raw === "sent") return "completed";
      return raw || "completed";
    }

    function syncRecentProcessing(data) {
      const now = Date.now();
      const activeIds = new Set();

      (data.processing || []).forEach(task => {
        activeIds.add(String(task.id));
        recentProcessingState[String(task.id)] = {
          task: task,
          mode: "processing",
          finalStatus: "",
          expiresAt: 0,
        };
      });

      (data.recent_finalized || []).forEach(task => {
        const key = String(task.id);
        if (activeIds.has(key)) {
          return;
        }

        const updatedAt = task.updated_at ? new Date(task.updated_at).getTime() : now;
        const initialExpiresAt = updatedAt + (processingHoldSeconds * 1000);
        if (initialExpiresAt <= now) {
          delete recentProcessingState[key];
          return;
        }

        const existing = recentProcessingState[key];
        const mergedTask = Object.assign({}, existing && existing.task ? existing.task : {}, task);
        if ((!mergedTask.available_at || String(mergedTask.available_at).startsWith("0001-01-01")) && existing && existing.task && existing.task.available_at) {
          mergedTask.available_at = existing.task.available_at;
        }
        if ((!mergedTask.created_at || String(mergedTask.created_at).startsWith("0001-01-01")) && existing && existing.task && existing.task.created_at) {
          mergedTask.created_at = existing.task.created_at;
        }

        recentProcessingState[key] = {
          task: mergedTask,
          mode: "finalized",
          finalStatus: normalizeRecentStatus(task.status),
          expiresAt: existing && existing.mode === "finalized" && existing.expiresAt > now
            ? existing.expiresAt
            : initialExpiresAt,
        };
      });

      Object.keys(recentProcessingState).forEach(key => {
        const item = recentProcessingState[key];
        if (!item) {
          return;
        }
        if (item.mode === "processing") {
          if (!activeIds.has(key)) {
            item.mode = "finalized";
            item.finalStatus = item.finalStatus || "completed";
            item.expiresAt = now + (processingHoldSeconds * 1000);
          }
          return;
        }
        if (item.expiresAt <= now) {
          delete recentProcessingState[key];
        }
      });
    }

    function buildProcessingRows(data) {
      syncRecentProcessing(data);

      const now = Date.now();
      const rows = [];
      const statusCountdowns = {};

      Object.values(recentProcessingState).forEach(item => {
        if (!item || !item.task) {
          return;
        }

        const task = Object.assign({}, item.task);
        if (item.mode === "finalized") {
          task.status = item.finalStatus;
          statusCountdowns[task.id] = Math.max(0, Math.ceil((item.expiresAt - now) / 1000));
        }

        rows.push(task);
      });

      rows.sort((a, b) => {
        const aProcessing = String(a.status).toLowerCase() === "processing" ? 0 : 1;
        const bProcessing = String(b.status).toLowerCase() === "processing" ? 0 : 1;
        if (aProcessing !== bProcessing) {
          return aProcessing - bProcessing;
        }
        return new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0);
      });

      return {
        rows: rows,
        statusCountdowns: statusCountdowns,
      };
    }

    function renderLoadingState(label) {
      return '<div class="loading-state"><div class="spinner"></div><div>' + escapeHtml(label) + '</div></div>';
    }

    function renderEmptyState(label) {
      return '<div class="empty-state">' +
        '<div class="empty-icon">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' +
        '<path d="M3 7.5 5.2 5h13.6L21 7.5"></path>' +
        '<path d="M4 8h16v9.5A2.5 2.5 0 0 1 17.5 20h-11A2.5 2.5 0 0 1 4 17.5Z"></path>' +
        '<path d="M9 12h6"></path>' +
        '</svg>' +
        '</div>' +
        '<div>' + escapeHtml(label) + '</div>' +
        '</div>';
    }

    function renderPager(page, totalPages, totalItems, tabId) {
      return '<div class="pager">' +
        '<div class="pager-info">Página ' + page + ' de ' + totalPages + ' · ' + totalItems + ' registros</div>' +
        '<div class="pager-controls">' +
        '<button ' + (page <= 1 ? 'disabled' : '') + ' onclick="changeGroupPage(\'' + tabId + '\', -1)">Anterior</button>' +
        '<button ' + (page >= totalPages ? 'disabled' : '') + ' onclick="changeGroupPage(\'' + tabId + '\', 1)">Próxima</button>' +
        '</div>' +
        '</div>';
    }

    function renderGroupTable(tabId, group) {
      const totalItems = group.tasks.length;
      const totalPages = Math.max(1, Math.ceil(totalItems / queuePageSize));
      const currentPage = Math.min(queueGroupPages[tabId] || 1, totalPages);
      queueGroupPages[tabId] = currentPage;
      const start = (currentPage - 1) * queuePageSize;
      const pageTasks = group.tasks.slice(start, start + queuePageSize);
      return renderTaskTable(pageTasks, "Sem registros") + renderPager(currentPage, totalPages, totalItems, tabId);
    }

    function renderGroups(groups, emptyLabel) {
      const totalScheduled = (groups || []).reduce((total, group) => total + ((group.tasks || []).length), 0);
      let html = "<h2 style='margin-top:2rem;'>Agendados (" + totalScheduled + ")</h2>";
      if (!groups || groups.length === 0) {
        return html + renderEmptyState(emptyLabel);
      }

      html += "<div class='tabs'>";
      groups.forEach((group, index) => {
        const tabId = "future-" + index;
        const isActive = activeQueueTab ? activeQueueTab === tabId : index === 0;
        html += "<button class='tab-button" + (isActive ? " active" : "") + "' data-tab-target='" + tabId + "'>" + escapeHtml(group.label) + "</button>";
      });
      html += "</div>";

      groups.forEach((group, index) => {
        const tabId = "future-" + index;
        const isActive = activeQueueTab ? activeQueueTab === tabId : index === 0;
        html += "<div class='tab-panel" + (isActive ? " active" : "") + "' id='" + tabId + "'>";
        html += renderGroupTable(tabId, group);
        html += "</div>";
      });

      return html;
    }

    function bindTabs() {
      document.querySelectorAll(".tab-button").forEach(button => {
        button.addEventListener("click", function () {
          const parent = button.parentElement;
          if (!parent) return;
          parent.querySelectorAll(".tab-button").forEach(item => item.classList.remove("active"));
          button.classList.add("active");
          activeQueueTab = button.getAttribute("data-tab-target") || "";

          const container = parent.parentElement;
          if (!container) return;
          container.querySelectorAll(".tab-panel").forEach(panel => panel.classList.remove("active"));
          const panel = container.querySelector("#" + activeQueueTab);
          if (panel) panel.classList.add("active");
        });
      });
    }

    function ensureActiveTab() {
      if (!activeQueueTab) return;

      const activeButton = document.querySelector('.tab-button[data-tab-target="' + activeQueueTab + '"]');
      const activePanel = document.getElementById(activeQueueTab);
      if (activeButton && activePanel) {
        activeButton.classList.add("active");
        activePanel.classList.add("active");
        return;
      }

      const fallbackButton = document.querySelector(".tab-button");
      if (!fallbackButton) return;

      activeQueueTab = fallbackButton.getAttribute("data-tab-target") || "";
      fallbackButton.classList.add("active");
      const fallbackPanel = document.getElementById(activeQueueTab);
      if (fallbackPanel) fallbackPanel.classList.add("active");
    }

    function changeGroupPage(tabId, delta) {
      const currentPage = queueGroupPages[tabId] || 1;
      queueGroupPages[tabId] = Math.max(1, currentPage + delta);
      updateActiveTable();
    }

    function renderActiveQueues(data) {
      const source = data || latestActiveQueuesData || { processing: [], pending_future: [], recent_finalized: [] };
      let html = "";
      const processingView = buildProcessingRows(source);
      html += "<h2>Processing (" + processingView.rows.length + ")</h2>";
      html += "<div class='processing-shell" + (processingView.rows.length === 0 ? " is-empty" : "") + "'>";
      html += renderTaskTable(processingView.rows, "Nenhuma task em processing.", { statusCountdowns: processingView.statusCountdowns });
      html += "</div>";
      html += renderGroups(source.pending_future, "Nenhuma task agendada.");
      document.getElementById("activeQueuesContent").innerHTML = html;
      bindTabs();
      ensureActiveTab();
      activeQueuesFirstLoad = false;
    }

    function updateActiveTable() {
      if (activeQueuesFirstLoad) {
        document.getElementById("activeQueuesContent").innerHTML = renderLoadingState("Carregando tasks...");
      }
      fetch('/api/queues?auth={{.AuthKey}}')
        .then(response => response.json())
        .then(data => {
          latestActiveQueuesData = data;
          renderActiveQueues(data);
        })
        .catch(err => console.error(err));
    }

    setInterval(updateActiveTable, 5000);
    setInterval(function () {
      if (!latestActiveQueuesData) {
        return;
      }
      renderActiveQueues();
    }, 1000);
    updateActiveTable();
  </script>
{{end}}
`

// Template para a tabela de filas com falha com atualização via AJAX.
const failedTemplate = `
{{define "content"}}
  <div class="section-head">
    <div><h1>Falhas do Worker</h1></div>
    <a class="button-danger" href="/clear/failed?auth={{.AuthKey}}&redirect=/queues_failed" data-confirm-action="true" data-confirm-message="Limpar todas as tasks com falha?">Limpar</a>
  </div>
  <div id="failedShell" class="table-shell">
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Queue Name</th>
          <th>Task Name</th>
          <th>Status</th>
          <th>Exception</th>
          <th>Created At</th>
          <th>Updated At</th>
          <th>Ação</th>
        </tr>
      </thead>
      <tbody id="failedTableBody"></tbody>
    </table>
    <div id="failedPager"></div>
  </div>

  <div id="exceptionModal" class="exception-modal">
    <div class="exception-panel">
      <h3>Exceção Completa</h3>
      <pre id="exceptionContent"></pre>
      <div style="margin-top:1rem;text-align:right;">
        <button class="button-secondary" onclick="closeException()">Fechar</button>
      </div>
    </div>
  </div>

  <script>
    let failedPage = {{.Pagination.Page}};
    const failedPageSize = {{.Pagination.PageSize}};
    let failedFirstLoad = true;

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function renderStatusBadge(status) {
      const raw = String(status || "").trim();
      const normalized = raw.toLowerCase().replace(/[^a-z]/g, "");
      return '<span class="status-badge ' + escapeHtml(normalized || "failed") + '">' + escapeHtml(raw || "-") + '</span>';
    }

    function renderLoadingState(label) {
      return '<div class="loading-state"><div class="spinner"></div><div>' + escapeHtml(label) + '</div></div>';
    }

    function renderEmptyState(label) {
      return '<div class="empty-state">' +
        '<div class="empty-icon">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' +
        '<path d="M3 7.5 5.2 5h13.6L21 7.5"></path>' +
        '<path d="M4 8h16v9.5A2.5 2.5 0 0 1 17.5 20h-11A2.5 2.5 0 0 1 4 17.5Z"></path>' +
        '<path d="M9 12h6"></path>' +
        '</svg>' +
        '</div>' +
        '<div>' + escapeHtml(label) + '</div>' +
        '</div>';
    }

    function showException(button) {
      const content = button.getAttribute('data-exception');
      document.getElementById('exceptionContent').textContent = content;
      document.getElementById('exceptionModal').style.display = 'flex';
    }

    function closeException() {
      document.getElementById('exceptionModal').style.display = 'none';
    }

    function renderExceptionCell(task) {
      if (!task.exception) return "<td>-</td>";
      if (task.exception.length <= 34) {
        return "<td>" + escapeHtml(task.exception) + "</td>";
      }
      return "<td>" + escapeHtml(task.exception.substring(0, 34)) + "...</td>";
    }

    function renderActionCell(task) {
      if (!task.exception) return "<td>-</td>";
      return '<td><button class="icon-button" onclick="showException(this)" data-exception="' + escapeHtml(task.exception) + '" aria-label="Ver falha" title="Ver falha">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' +
        '<path d="M1.5 12s3.8-6.5 10.5-6.5S22.5 12 22.5 12 18.7 18.5 12 18.5 1.5 12 1.5 12Z"></path>' +
        '<circle cx="12" cy="12" r="3"></circle>' +
        '</svg>' +
        '</button></td>';
    }

    function renderPager(targetId, pagination, changeFnName) {
      const target = document.getElementById(targetId);
      if (!target) return;
      target.innerHTML = '<div class="pager">' +
        '<div class="pager-info">Página ' + pagination.page + ' de ' + pagination.total_pages + ' · ' + pagination.total_items + ' registros</div>' +
        '<div class="pager-controls">' +
        '<button ' + (pagination.page <= 1 ? 'disabled' : '') + ' onclick="' + changeFnName + '(-1)">Anterior</button>' +
        '<button ' + (pagination.page >= pagination.total_pages ? 'disabled' : '') + ' onclick="' + changeFnName + '(1)">Próxima</button>' +
        '</div>' +
        '</div>';
    }

    function updateFailedTable() {
      if (failedFirstLoad) {
        document.getElementById('failedShell').innerHTML = renderLoadingState("Carregando falhas...");
      }
      fetch('/api/queues_failed?auth={{.AuthKey}}&page=' + failedPage + '&page_size=' + failedPageSize)
        .then(res => res.json())
        .then(data => {
          const shell = document.getElementById('failedShell');
          if (!data.tasks || data.tasks.length === 0) {
            shell.innerHTML = renderEmptyState("Nao ha registros.");
            return;
          }

          shell.innerHTML =
            '<table><thead><tr>' +
            '<th>ID</th><th>Queue Name</th><th>Task Name</th><th>Status</th><th>Exception</th><th>Created At</th><th>Updated At</th><th>Ação</th>' +
            '</tr></thead><tbody id="failedTableBody"></tbody></table><div id="failedPager"></div>';
          failedPage = data.pagination.page;
          const tbody = document.getElementById('failedTableBody');
          let rows = '';
          data.tasks.forEach(task => {
            const encodedTask = encodeTaskPayload(task);
            rows += '<tr>';
            rows += '<td>' + task.id + '</td>';
            rows += '<td><a href="#" class="queue-link" data-task="' + escapeHtml(encodedTask) + '">' + escapeHtml(task.queue_name) + '</a></td>';
            rows += '<td>' + escapeHtml(task.task_name) + '</td>';
            rows += '<td>' + renderStatusBadge(task.status) + '</td>';
            rows += renderExceptionCell(task);
            rows += '<td>' + new Date(task.created_at).toLocaleString() + '</td>';
            rows += '<td>' + new Date(task.updated_at).toLocaleString() + '</td>';
            rows += renderActionCell(task);
            rows += '</tr>';
          });
          tbody.innerHTML = rows;
          renderPager('failedPager', data.pagination, 'changeFailedPage');
          failedFirstLoad = false;
        })
        .catch(console.error);
    }

    function changeFailedPage(delta) {
      failedPage = Math.max(1, failedPage + delta);
      updateFailedTable();
    }

    setInterval(updateFailedTable, 5000);
    updateFailedTable();
  </script>
{{end}}
`

// Template para a tabela de filas completadas com atualização via AJAX.
const completedTemplate = `
{{define "content"}}
  <div class="section-head">
    <div><h1>Histórico Concluído</h1></div>
    <a class="button-danger" href="/clear/completed?auth={{.AuthKey}}&redirect=/queues_completed" data-confirm-action="true" data-confirm-message="Limpar todo o histórico de concluídas?">Limpar</a>
  </div>
  <div id="completedShell" class="table-shell">
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Queue Task ID</th>
          <th>Queue Name</th>
          <th>Task Name</th>
          <th>Status</th>
          <th>Completed At</th>
        </tr>
      </thead>
      <tbody id="completedTableBody"></tbody>
    </table>
    <div id="completedPager"></div>
  </div>
  <script>
    let completedPage = {{.Pagination.Page}};
    const completedPageSize = {{.Pagination.PageSize}};
    let completedFirstLoad = true;

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function renderStatusBadge(status) {
      const raw = String(status || "").trim();
      const normalized = raw.toLowerCase().replace(/[^a-z]/g, "");
      return '<span class="status-badge ' + escapeHtml(normalized || "completed") + '">' + escapeHtml(raw || "-") + '</span>';
    }

    function renderLoadingState(label) {
      return '<div class="loading-state"><div class="spinner"></div><div>' + escapeHtml(label) + '</div></div>';
    }

    function renderEmptyState(label) {
      return '<div class="empty-state">' +
        '<div class="empty-icon">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' +
        '<path d="M3 7.5 5.2 5h13.6L21 7.5"></path>' +
        '<path d="M4 8h16v9.5A2.5 2.5 0 0 1 17.5 20h-11A2.5 2.5 0 0 1 4 17.5Z"></path>' +
        '<path d="M9 12h6"></path>' +
        '</svg>' +
        '</div>' +
        '<div>' + escapeHtml(label) + '</div>' +
        '</div>';
    }

    function renderPager(targetId, pagination, changeFnName) {
      const target = document.getElementById(targetId);
      if (!target) return;
      target.innerHTML = '<div class="pager">' +
        '<div class="pager-info">Página ' + pagination.page + ' de ' + pagination.total_pages + ' · ' + pagination.total_items + ' registros</div>' +
        '<div class="pager-controls">' +
        '<button ' + (pagination.page <= 1 ? 'disabled' : '') + ' onclick="' + changeFnName + '(-1)">Anterior</button>' +
        '<button ' + (pagination.page >= pagination.total_pages ? 'disabled' : '') + ' onclick="' + changeFnName + '(1)">Próxima</button>' +
        '</div>' +
        '</div>';
    }

    function updateCompletedTable() {
      if (completedFirstLoad) {
        document.getElementById('completedShell').innerHTML = renderLoadingState("Carregando concluidas...");
      }
      fetch('/api/queues_completed?auth={{.AuthKey}}&page=' + completedPage + '&page_size=' + completedPageSize)
        .then(response => response.json())
        .then(data => {
          const shell = document.getElementById('completedShell');
          if (!data.tasks || data.tasks.length === 0) {
            shell.innerHTML = renderEmptyState("Nao ha registros.");
            return;
          }

          shell.innerHTML =
            '<table><thead><tr>' +
            '<th>ID</th><th>Queue Task ID</th><th>Queue Name</th><th>Task Name</th><th>Status</th><th>Completed At</th>' +
            '</tr></thead><tbody id="completedTableBody"></tbody></table><div id="completedPager"></div>';
          completedPage = data.pagination.page;
          let tbody = document.getElementById("completedTableBody");
          let rows = "";
          data.tasks.forEach(task => {
            const encodedTask = encodeTaskPayload(task);
            rows += "<tr>";
            rows += "<td>" + task.id + "</td>";
            rows += "<td>" + task.queue_task_id + "</td>";
            rows += '<td><a href="#" class="queue-link" data-task="' + escapeHtml(encodedTask) + '">' + escapeHtml(task.queue_name) + '</a></td>';
            rows += "<td>" + escapeHtml(task.task_name) + "</td>";
            rows += "<td>" + renderStatusBadge(task.status) + "</td>";
            rows += "<td>" + new Date(task.completed_at).toLocaleString() + "</td>";
            rows += "</tr>";
          });
          tbody.innerHTML = rows;
          renderPager('completedPager', data.pagination, 'changeCompletedPage');
          completedFirstLoad = false;
        })
        .catch(err => console.error(err));
    }

    function changeCompletedPage(delta) {
      completedPage = Math.max(1, completedPage + delta);
      updateCompletedTable();
    }

    setInterval(updateCompletedTable, 5000);
    updateCompletedTable();
  </script>
{{end}}
`

// ---------------------
// Handlers de Visualização
// ---------------------

// HomeHandler renderiza a página inicial com os cards (atualizados via AJAX).
func HomeHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	activeCount, err := countActiveQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao contar filas ativas", http.StatusInternalServerError)
		return
	}
	failedCount, err := countFailedQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao contar filas com falha", http.StatusInternalServerError)
		return
	}
	completedCount, err := countCompletedQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao contar filas completadas", http.StatusInternalServerError)
		return
	}
	data := struct {
		Title          string
		AuthKey        string
		Page           string
		ActiveCount    int
		FailedCount    int
		CompletedCount int
	}{
		Title:          "Dashboard - WEBTECH QUEUES",
		AuthKey:        r.URL.Query().Get("auth"),
		Page:           "home",
		ActiveCount:    activeCount,
		FailedCount:    failedCount,
		CompletedCount: completedCount,
	}
	tmpl, err := template.New("base").Parse(baseTemplate)
	if err != nil {
		http.Error(w, "Erro ao criar template base", http.StatusInternalServerError)
		return
	}
	tmpl, err = tmpl.Parse(homeTemplateCards)
	if err != nil {
		http.Error(w, "Erro ao criar template home", http.StatusInternalServerError)
		return
	}
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Erro ao executar template", http.StatusInternalServerError)
	}
}

// QueueHandler renderiza a página de filas ativas (pending/processing).
func QueueHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	data := struct {
		Title   string
		AuthKey string
		Page    string
	}{
		Title:   "Queues Pending - WEBTECH QUEUES",
		AuthKey: r.URL.Query().Get("auth"),
		Page:    "queues",
	}
	tmpl, err := template.New("base").Parse(baseTemplate)
	if err != nil {
		http.Error(w, "Erro ao criar template base", http.StatusInternalServerError)
		return
	}
	tmpl, err = tmpl.Parse(queuesTemplate)
	if err != nil {
		http.Error(w, "Erro ao criar template de queues", http.StatusInternalServerError)
		return
	}
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Erro ao executar template", http.StatusInternalServerError)
	}
}

// FailedQueueHandler renderiza a página de filas com falha.
func FailedQueueHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	page, pageSize := getPaginationParams(r, 50)
	tasks, pagination, err := fetchFailedQueueTasks(page, pageSize)
	if err != nil {
		http.Error(w, "Erro ao buscar filas com falha", http.StatusInternalServerError)
		return
	}
	data := struct {
		Title      string
		Tasks      []QueueTask
		Pagination Pagination
		AuthKey    string
		Page       string
	}{
		Title:      "Queues Failed - WEBTECH QUEUES",
		Tasks:      tasks,
		Pagination: pagination,
		AuthKey:    r.URL.Query().Get("auth"),
		Page:       "failed",
	}
	tmpl, err := template.New("base").Parse(baseTemplate)
	if err != nil {
		http.Error(w, "Erro ao criar template base", http.StatusInternalServerError)
		return
	}
	tmpl, err = tmpl.Parse(failedTemplate)
	if err != nil {
		http.Error(w, "Erro ao criar template de queues failed", http.StatusInternalServerError)
		return
	}
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Erro ao executar template", http.StatusInternalServerError)
	}
}

// CompletedHandler renderiza a página de filas completadas.
func CompletedHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	page, pageSize := getPaginationParams(r, 50)
	tasks, pagination, err := fetchCompletedTasks(page, pageSize)
	if err != nil {
		http.Error(w, "Erro ao buscar filas completadas", http.StatusInternalServerError)
		return
	}
	data := struct {
		Title      string
		Tasks      []CompletedTask
		Pagination Pagination
		AuthKey    string
		Page       string
	}{
		Title:      "Queues Completadas - WEBTECH QUEUES",
		Tasks:      tasks,
		Pagination: pagination,
		AuthKey:    r.URL.Query().Get("auth"),
		Page:       "completed",
	}
	tmpl, err := template.New("base").Parse(baseTemplate)
	if err != nil {
		http.Error(w, "Erro ao criar template base", http.StatusInternalServerError)
		return
	}
	tmpl, err = tmpl.Parse(completedTemplate)
	if err != nil {
		http.Error(w, "Erro ao criar template de queues completadas", http.StatusInternalServerError)
		return
	}
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Erro ao executar template", http.StatusInternalServerError)
	}
}

// ---------------------
// Handlers da API
// ---------------------

func APICountsHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	active, err := countActiveQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao contar filas ativas", http.StatusInternalServerError)
		return
	}
	failed, err := countFailedQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao contar filas com falha", http.StatusInternalServerError)
		return
	}
	completed, err := countCompletedQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao contar filas completadas", http.StatusInternalServerError)
		return
	}
	counts := map[string]int{
		"active":    active,
		"failed":    failed,
		"completed": completed,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(counts)
}

func APIQueuesHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	tasks, err := fetchActiveQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao buscar filas ativas", http.StatusInternalServerError)
		return
	}
	view := groupActiveQueueTasks(tasks)
	recentFinalized, err := fetchRecentFinalizedQueueTasks(15 * time.Second)
	if err == nil {
		view.RecentFinalized = recentFinalized
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(view)
}

func APIQueuesFailedHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	page, pageSize := getPaginationParams(r, 50)
	tasks, pagination, err := fetchFailedQueueTasks(page, pageSize)
	if err != nil {
		http.Error(w, "Erro ao buscar filas com falha", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(FailedTasksResponse{
		Tasks:      tasks,
		Pagination: pagination,
	})
}

func APICompletedQueuesHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	page, pageSize := getPaginationParams(r, 50)
	tasks, pagination, err := fetchCompletedTasks(page, pageSize)
	if err != nil {
		http.Error(w, "Erro ao buscar filas completadas", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(CompletedTasksResponse{
		Tasks:      tasks,
		Pagination: pagination,
	})
}

// ---------------------
// Função para obter o IP local (caso DOMAIN não esteja definido)
// ---------------------

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "localhost"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "localhost"
}

// ---------------------
// Configuração das Rotas e Início do Servidor
// ---------------------

func Start() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}
	auth := os.Getenv("AUTH")

	// Se a variável DOMAIN estiver definida, usa-a; caso contrário, detecta o IP local.
	domain := os.Getenv("DOMAIN")
	if domain == "" {
		domain = getLocalIP()
	}

	// Imprime a URL de acesso
	fmt.Printf("Servidor web rodando na porta %s\n", port)
	fmt.Printf("Acesse: http://%s:%s/?auth=%s\n", domain, port, auth)

	http.HandleFunc("/logo-queu.png", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "/home/newgestor-npanel/htdocs/queue-work-go/web/logo-queu.png")
	})
	http.HandleFunc("/queue-icon.png", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "/home/newgestor-npanel/htdocs/queue-work-go/web/queue-icon.png")
	})

	// Rotas de visualização
	http.HandleFunc("/", HomeHandler)
	http.HandleFunc("/queues", QueueHandler)
	http.HandleFunc("/queues_failed", FailedQueueHandler)
	http.HandleFunc("/queues_completed", CompletedHandler)
	// Rotas de limpeza
	http.HandleFunc("/clear/active", ClearActiveHandler)
	http.HandleFunc("/clear/failed", ClearFailedHandler)
	http.HandleFunc("/clear/completed", ClearCompletedHandler)
	// Rotas da API
	http.HandleFunc("/api/counts", APICountsHandler)
	http.HandleFunc("/api/queues", APIQueuesHandler)
	http.HandleFunc("/api/queues_failed", APIQueuesFailedHandler)
	http.HandleFunc("/api/queues_completed", APICompletedQueuesHandler)

	log.Fatal(http.ListenAndServe(":"+port, nil))
}
