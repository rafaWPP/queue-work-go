package web

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ---------------------
// Tipos e Conexão
// ---------------------

// QueueTask representa um registro da tabela queue_tasks.
type QueueTask struct {
	ID        int       `json:"id"`
	QueueName string    `json:"queue_name"`
	TaskName  string    `json:"task_name"`
	Data      string    `json:"data"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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

// getDSN monta a string de conexão utilizando variáveis de ambiente.
func getDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
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
	query := "SELECT id, queue_name, task_name, data, status, created_at, updated_at FROM queue_tasks WHERE status IN ('pending', 'processing')"
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]QueueTask, 0)
	for rows.Next() {
		var t QueueTask
		var queueName, taskName, data, status sql.NullString
		if err := rows.Scan(&t.ID, &queueName, &taskName, &data, &status, &t.CreatedAt, &t.UpdatedAt); err != nil {
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
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func fetchFailedQueueTasks() ([]QueueTask, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	query := "SELECT id, queue_name, task_name, data, status, created_at, updated_at FROM queue_tasks WHERE status = 'failed'"
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]QueueTask, 0)
	for rows.Next() {
		var t QueueTask
		var queueName, taskName, data, status sql.NullString
		if err := rows.Scan(&t.ID, &queueName, &taskName, &data, &status, &t.CreatedAt, &t.UpdatedAt); err != nil {
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
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func fetchCompletedTasks() ([]CompletedTask, error) {
	dsn := getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	query := "SELECT id, queue_task_id, queue_name, task_name, data, status, exception, completed_at FROM queue_tasks_completed"
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
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
	return tasks, nil
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
	http.Redirect(w, r, "/?auth="+r.URL.Query().Get("auth"), http.StatusSeeOther)
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
	http.Redirect(w, r, "/?auth="+r.URL.Query().Get("auth"), http.StatusSeeOther)
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
	http.Redirect(w, r, "/?auth="+r.URL.Query().Get("auth"), http.StatusSeeOther)
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
  <style>
    body {
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      background: #f3f4f6;
      margin: 0;
      padding: 0;
      color: #374151;
    }
    .navbar {
      background-color: #1d4ed8;
      padding: 1rem;
      color: #fff;
      text-align: center;
    }
    .navbar a {
      color: #fff;
      margin: 0 1rem;
      text-decoration: none;
      font-weight: bold;
    }
    .navbar button {
      margin-left: 1rem;
      background-color: #2563eb;
      border: none;
      padding: 0.5rem 1rem;
      color: #fff;
      border-radius: 0.25rem;
      cursor: pointer;
    }
    .container {
      max-width: 1200px;
      margin: 2rem auto;
      padding: 0 1rem;
    }
    .card {
      background: #fff;
      border-radius: 0.5rem;
      box-shadow: 0 1px 3px rgba(0,0,0,0.1);
      padding: 1.5rem;
      margin-bottom: 2rem;
    }
    h1 {
      font-size: 1.5rem;
      margin-bottom: 1rem;
      text-align: center;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 0.75rem;
      text-align: left;
      border-bottom: 1px solid #e5e7eb;
    }
    th {
      background-color: #f9fafb;
      font-weight: 600;
    }
    tr:nth-child(even) {
      background-color: #f3f4f6;
    }
  </style>
</head>
<body>
  <div class="navbar">
    <a href="/?auth={{.AuthKey}}">Home</a>
    <a href="/queues?auth={{.AuthKey}}">Queues Pending</a>
    <a href="/queues_failed?auth={{.AuthKey}}">Queues Failed</a>
    <a href="/queues_completed?auth={{.AuthKey}}">Queues Completed</a>
    <button onclick="location.reload()">Atualizar</button>
  </div>
  <div class="container">
    <div class="card">
      {{template "content" .}}
    </div>
  </div>
</body>
</html>
`

// Home template com 3 cards e atualização automática dos números via AJAX.
const homeTemplateCards = `
{{define "content"}}
  <h1>Dashboard WEBTECH QUEUES</h1>
  <div style="display: flex; flex-wrap: wrap; gap: 1rem; justify-content: center;">
    <div style="flex: 1; min-width: 250px; background: rgb(0,190,237); padding: 1rem; border-radius: 8px; text-align: center;">
      <h2>Queues Pending</h2>
      <p>Total: <span id="activeCount">{{.ActiveCount}}</span></p>
      <a href="/clear/active?auth={{.AuthKey}}" style="text-decoration: none;">
         <button style="padding: 0.5rem 1rem; background: #00796b; color: #fff; border: none; border-radius: 4px;">Limpar</button>
      </a>
    </div>
    <div style="flex: 1; min-width: 250px; background: #ffccbc; padding: 1rem; border-radius: 8px; text-align: center;">
      <h2>Queues Failed</h2>
      <p>Total: <span id="failedCount">{{.FailedCount}}</span></p>
      <a href="/clear/failed?auth={{.AuthKey}}" style="text-decoration: none;">
         <button style="padding: 0.5rem 1rem; background: #d32f2f; color: #fff; border: none; border-radius: 4px;">Limpar</button>
      </a>
    </div>
    <div style="flex: 1; min-width: 250px; background: #c5e1a5; padding: 1rem; border-radius: 8px; text-align: center;">
      <h2>Queues Completed</h2>
      <p>Total: <span id="completedCount">{{.CompletedCount}}</span></p>
      <a href="/clear/completed?auth={{.AuthKey}}" style="text-decoration: none;">
         <button style="padding: 0.5rem 1rem; background: #388e3c; color: #fff; border: none; border-radius: 4px;">Limpar</button>
      </a>
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
  <h1>Queues Pending</h1>
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Queue Name</th>
        <th>Task Name</th>
        <th>Status</th>
        <th>Created At</th>
        <th>Updated At</th>
      </tr>
    </thead>
    <tbody id="activeTableBody">
      {{range .Tasks}}
      <tr>
        <td>{{.ID}}</td>
        <td>{{.QueueName}}</td>
        <td>{{.TaskName}}</td>
        <td>{{.Status}}</td>
        <td>{{.CreatedAt.Format "2006-01-02 15:04:05"}}</td>
        <td>{{.UpdatedAt.Format "2006-01-02 15:04:05"}}</td>
      </tr>
      {{else}}
      <tr>
        <td colspan="6" style="text-align: center;">Nenhuma fila ativa encontrada.</td>
      </tr>
      {{end}}
    </tbody>
  </table>
  <script>
    function updateActiveTable() {
      fetch('/api/queues?auth={{.AuthKey}}')
        .then(response => response.json())
        .then(data => {
          let tbody = document.getElementById("activeTableBody");
          let rows = "";
          data.forEach(task => {
            rows += "<tr>";
            rows += "<td>" + task.id + "</td>";
            rows += "<td>" + task.queue_name + "</td>";
            rows += "<td>" + task.task_name + "</td>";
            rows += "<td>" + task.status + "</td>";
            rows += "<td>" + new Date(task.created_at).toLocaleString() + "</td>";
            rows += "<td>" + new Date(task.updated_at).toLocaleString() + "</td>";
            rows += "</tr>";
          });
          if (rows === "") {
            rows = '<tr><td colspan="6" style="text-align: center;">Nenhuma fila ativa encontrada.</td></tr>';
          }
          tbody.innerHTML = rows;
        })
        .catch(err => console.error(err));
    }
    setInterval(updateActiveTable, 5000);
    updateActiveTable();
  </script>
{{end}}
`

// Template para a tabela de filas com falha com atualização via AJAX.
const failedTemplate = `
{{define "content"}}
  <h1>Queues Failed</h1>
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Queue Name</th>
        <th>Task Name</th>
        <th>Status</th>
        <th>Created At</th>
        <th>Updated At</th>
      </tr>
    </thead>
    <tbody id="failedTableBody">
      {{range .Tasks}}
      <tr>
        <td>{{.ID}}</td>
        <td>{{.QueueName}}</td>
        <td>{{.TaskName}}</td>
        <td>{{.Status}}</td>
        <td>{{.CreatedAt.Format "2006-01-02 15:04:05"}}</td>
        <td>{{.UpdatedAt.Format "2006-01-02 15:04:05"}}</td>
      </tr>
      {{else}}
      <tr>
        <td colspan="6" style="text-align: center;">Nenhuma fila com falha encontrada.</td>
      </tr>
      {{end}}
    </tbody>
  </table>
  <script>
    function updateFailedTable() {
      fetch('/api/queues_failed?auth={{.AuthKey}}')
        .then(response => response.json())
        .then(data => {
          let tbody = document.getElementById("failedTableBody");
          let rows = "";
          data.forEach(task => {
            rows += "<tr>";
            rows += "<td>" + task.id + "</td>";
            rows += "<td>" + task.queue_name + "</td>";
            rows += "<td>" + task.task_name + "</td>";
            rows += "<td>" + task.status + "</td>";
            rows += "<td>" + new Date(task.created_at).toLocaleString() + "</td>";
            rows += "<td>" + new Date(task.updated_at).toLocaleString() + "</td>";
            rows += "</tr>";
          });
          if (rows === "") {
            rows = '<tr><td colspan="6" style="text-align: center;">Nenhuma fila com falha encontrada.</td></tr>';
          }
          tbody.innerHTML = rows;
        })
        .catch(err => console.error(err));
    }
    setInterval(updateFailedTable, 5000);
    updateFailedTable();
  </script>
{{end}}
`

// Template para a tabela de filas completadas com atualização via AJAX.
const completedTemplate = `
{{define "content"}}
  <h1>Queues Completadas</h1>
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Queue Task ID</th>
        <th>Queue Name</th>
        <th>Task Name</th>
        <th>Status</th>
        <th>Exception</th>
        <th>Completed At</th>
      </tr>
    </thead>
    <tbody id="completedTableBody">
      {{range .Tasks}}
      <tr>
        <td>{{.ID}}</td>
        <td>{{.QueueTaskID}}</td>
        <td>{{.QueueName}}</td>
        <td>{{.TaskName}}</td>
        <td>{{.Status}}</td>
        <td>{{.Exception}}</td>
        <td>{{.CompletedAt.Format "2006-01-02 15:04:05"}}</td>
      </tr>
      {{else}}
      <tr>
        <td colspan="7" style="text-align: center;">Nenhuma fila completada encontrada.</td>
      </tr>
      {{end}}
    </tbody>
  </table>
  <script>
    function updateCompletedTable() {
      fetch('/api/queues_completed?auth={{.AuthKey}}')
        .then(response => response.json())
        .then(data => {
          let tbody = document.getElementById("completedTableBody");
          let rows = "";
          data.forEach(task => {
            rows += "<tr>";
            rows += "<td>" + task.id + "</td>";
            rows += "<td>" + task.queue_task_id + "</td>";
            rows += "<td>" + task.queue_name + "</td>";
            rows += "<td>" + task.task_name + "</td>";
            rows += "<td>" + task.status + "</td>";
            rows += "<td>" + task.exception + "</td>";
            rows += "<td>" + new Date(task.completed_at).toLocaleString() + "</td>";
            rows += "</tr>";
          });
          if (rows === "") {
            rows = '<tr><td colspan="7" style="text-align: center;">Nenhuma fila completada encontrada.</td></tr>';
          }
          tbody.innerHTML = rows;
        })
        .catch(err => console.error(err));
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
		ActiveCount    int
		FailedCount    int
		CompletedCount int
	}{
		Title:          "Dashboard - WEBTECH QUEUES",
		AuthKey:        r.URL.Query().Get("auth"),
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
	tasks, err := fetchActiveQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao buscar filas ativas", http.StatusInternalServerError)
		return
	}
	data := struct {
		Title   string
		Tasks   []QueueTask
		AuthKey string
	}{
		Title:   "Queues Pending - WEBTECH QUEUES",
		Tasks:   tasks,
		AuthKey: r.URL.Query().Get("auth"),
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
	tasks, err := fetchFailedQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao buscar filas com falha", http.StatusInternalServerError)
		return
	}
	data := struct {
		Title   string
		Tasks   []QueueTask
		AuthKey string
	}{
		Title:   "Queues Failed - WEBTECH QUEUES",
		Tasks:   tasks,
		AuthKey: r.URL.Query().Get("auth"),
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
	tasks, err := fetchCompletedTasks()
	if err != nil {
		http.Error(w, "Erro ao buscar filas completadas", http.StatusInternalServerError)
		return
	}
	data := struct {
		Title   string
		Tasks   []CompletedTask
		AuthKey string
	}{
		Title:   "Queues Completadas - WEBTECH QUEUES",
		Tasks:   tasks,
		AuthKey: r.URL.Query().Get("auth"),
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func APIQueuesFailedHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	tasks, err := fetchFailedQueueTasks()
	if err != nil {
		http.Error(w, "Erro ao buscar filas com falha", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func APICompletedQueuesHandler(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r) {
		return
	}
	tasks, err := fetchCompletedTasks()
	if err != nil {
		http.Error(w, "Erro ao buscar filas completadas", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
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
