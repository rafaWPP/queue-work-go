// main.go
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"plugin"
	"sort"
	"strings"
	"time"
	"go-queue/web" 

	"go-queue/common" // IMPORTANTE: use o mesmo caminho definido no go.mod
	_ "github.com/go-sql-driver/mysql"
	"github.com/fatih/color"
	"github.com/joho/godotenv"
)

// Config é a estrutura para carregar a configuração das filas.
type Config struct {
	Queues []string `json:"queues"`
}

// loadEnvConfig carrega as variáveis do arquivo .env (para DB e outras).
func loadEnvConfig() (string, string, string, string, string, error) {
	err := godotenv.Load()
	if err != nil {
		logFatal("erro ao carregar arquivo .env")
		return "", "", "", "", "", fmt.Errorf("erro ao carregar arquivo .env")
	}

	DBUser := os.Getenv("DB_USER")
	DBPassword := os.Getenv("DB_PASSWORD")
	DBName := os.Getenv("DB_NAME")
	DBHost := os.Getenv("DB_HOST")
	DBPort := os.Getenv("DB_PORT")

	if DBUser == "" || DBPassword == "" || DBName == "" || DBHost == "" || DBPort == "" {
		logFatal("faltando variáveis de ambiente para o banco de dados")
		return "", "", "", "", "", fmt.Errorf("faltando variáveis de ambiente para o banco de dados")
	}

	return DBUser, DBPassword, DBName, DBHost, DBPort, nil
}

// connectToDatabase conecta ao banco de dados.
func connectToDatabase(DBUser, DBPassword, DBName, DBHost, DBPort string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", DBUser, DBPassword, DBHost, DBPort, DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logError("erro ao conectar ao banco de dados: %v", err)
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		logError("erro ao testar a conexão com o banco de dados: %v", err)
		return nil, err
	}

	logInfo("Conectado com sucesso ao banco de dados!\n\n")
	return db, nil
}

// runMigrations executa as migrações que ainda não foram aplicadas.
func runMigrations(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS migrations_go (
			id INT AUTO_INCREMENT PRIMARY KEY,
			migration VARCHAR(255) UNIQUE,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return fmt.Errorf("erro ao criar tabela de migrações: %v", err)
	}

	migrationDir := "migrations"
	files, err := ioutil.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("erro ao ler pasta de migrações: %v", err)
	}

	var migrationFiles []fs.FileInfo
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sql") {
			migrationFiles = append(migrationFiles, file)
		}
	}
	sort.Slice(migrationFiles, func(i, j int) bool {
		return migrationFiles[i].Name() < migrationFiles[j].Name()
	})

	for _, file := range migrationFiles {
		migrationName := file.Name()
		var exists int
		err = db.QueryRow("SELECT COUNT(*) FROM migrations_go WHERE migration = ?", migrationName).Scan(&exists)
		if err != nil {
			return fmt.Errorf("erro ao verificar migração %s: %v", migrationName, err)
		}
		if exists > 0 {
			continue
		}

		path := filepath.Join(migrationDir, migrationName)
		sqlQuery, err := ioutil.ReadFile(path)
		if err != nil {
			logError("erro ao ler arquivo de migração %s: %v", migrationName, err)
			continue
		}

		_, err = db.Exec(string(sqlQuery))
		if err != nil {
			logError("erro ao executar migração %s: %v", migrationName, err)
			continue
		}

		_, err = db.Exec("INSERT INTO migrations_go (migration) VALUES (?)", migrationName)
		if err != nil {
			logError("erro ao registrar migração %s: %v", migrationName, err)
			continue
		}

		logInfo("---%s", migrationName)
		logInfo("---|___Sucesso\n\n")
	}

// Define um logo em ASCII centralizado
logo := `
██╗    ██╗███████╗██████╗ ████████╗███████╗ ██████╗██╗  ██╗   v.1.0.0
██║    ██║██╔════╝██╔══██╗╚══██╔══╝██╔════╝██╔════╝██║  ██║  
██║ █╗ ██║█████╗  ██████╔╝   ██║   █████╗  ██║     ███████║  
██║███╗██║██╔══╝  ██╔══██╗   ██║   ██╔══╝  ██║     ██╔══██║  
╚███╔███╔╝███████╗██████╔╝   ██║   ███████╗╚██████╗██║  ██║  
 ╚══╝╚══╝ ╚══════╝╚═════╝    ╚═╝   ╚══════╝ ╚═════╝╚═╝  ╚═╝
	        DEVELOPER BY RD-WEBTECH
`
 // Exibe o logo colorido
 logInfo(logo)
 logInfo("WebTech Queues started successfully\n\n")
	return nil
}

// loadQueuesConfig carrega a configuração das filas a partir de um arquivo JSON.
func loadQueuesConfig() ([]string, error) {
	configFile := "config.json"
	file, err := ioutil.ReadFile(configFile)
	if err != nil {
		logError("Erro ao ler arquivo de configuração: %v", err)
		return nil, fmt.Errorf("erro ao ler arquivo de configuração: %v", err)
	}

	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		logError("Erro ao parsear arquivo JSON: %v", err)
		return nil, fmt.Errorf("erro ao parsear arquivo JSON: %v", err)
	}

	return config.Queues, nil
}

// addTaskToQueue adiciona tarefa à fila.
// O campo task_name deve conter o caminho base do plugin (ex.: "jobs/sendmessage").
func addTaskToQueue(db *sql.DB, queueName, taskName, data string) error {
	query := "INSERT INTO queue_tasks (queue_name, task_name, data, status) VALUES (?, ?, ?, 'pending')"
	_, err := db.Exec(query, queueName, taskName, data)
	if err != nil {
		logError("erro ao adicionar tarefa à fila: %v", err)
		return fmt.Errorf("erro ao adicionar tarefa à fila: %v", err)
	}
	return nil
}

// formatDuration formata a duração para exibição em milissegundos com duas casas decimais.
func formatDuration(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)
	return fmt.Sprintf("%.2fms", ms)
}

// formatJobLog cria a linha de log com preenchimento de pontos até uma largura fixa.
func formatJobLog(job, suffix string) string {
	const fieldWidth = 50
	dotsCount := fieldWidth - len(job)
	if dotsCount < 0 {
		dotsCount = 0
	}
	dots := strings.Repeat(".", dotsCount)
	return fmt.Sprintf("%s %s %s", job, dots, suffix)
}

// processQueue processa uma tarefa de uma fila configurada.
func processQueue(db *sql.DB, queueName string) {
	rows, err := db.Query("SELECT id, task_name, data FROM queue_tasks WHERE status = 'pending' AND queue_name = ? LIMIT 1", queueName)
	if err != nil {
		logError("Erro ao buscar tarefas da fila %s: %v", queueName, err)
		return
	}
	defer rows.Close()

	if !rows.Next() {
		return
	}

	var taskID int
	var taskName, data string
	if err := rows.Scan(&taskID, &taskName, &data); err != nil {
		logError("Erro ao escanear tarefa na fila %s: %v", queueName, err)
		return
	}

	_, err = db.Exec("UPDATE queue_tasks SET status = 'processing' WHERE id = ?", taskID)
	if err != nil {
		logError("Erro ao atualizar tarefa para 'processing' na fila %s: %v", queueName, err)
		return
	}

	startTime := time.Now()
	logInfo(formatJobLog(taskName, "RUNNING"))

	pluginPath := fmt.Sprintf("%s.so", taskName)
	p, err := plugin.Open(pluginPath)
	if err != nil {
		common.LogJobError(db, taskID, taskName, fmt.Errorf("erro ao abrir plugin %s: %v", pluginPath, err))
		return
	}

	symNew, err := p.Lookup("New")
	if err != nil {
		common.LogJobError(db, taskID, taskName, fmt.Errorf("erro ao procurar símbolo New no plugin %s: %v", pluginPath, err))
		return
	}

	newFunc, ok := symNew.(func([]byte) common.Job)
	if !ok {
		common.LogJobError(db, taskID, taskName, fmt.Errorf("o símbolo New do plugin %s tem assinatura inválida", pluginPath))
		return
	}

	job := newFunc([]byte(data))
	if job == nil {
		common.LogJobError(db, taskID, taskName, fmt.Errorf("erro ao criar instância do job %s", taskName))
		return
	}

	processingError := job.Handle()
	elapsed := time.Since(startTime)

	if processingError != nil {
		common.LogJobError(db, taskID, taskName, processingError)
		logError(formatJobLog(taskName, fmt.Sprintf("%s FAIL", formatDuration(elapsed))))
	} else {
		_, err = db.Exec("UPDATE queue_tasks SET status = 'completed', exception = NULL WHERE id = ?", taskID)
		if err != nil {
			logError("Erro ao atualizar tarefa para 'completed' na fila %s: %v", queueName, err)
		}
		logInfo(formatJobLog(taskName, fmt.Sprintf("%s DONE", formatDuration(elapsed))))
	}
}

// processNonConfiguredQueue faz a mesma lógica de processQueue para filas não configuradas.
func processNonConfiguredQueue(db *sql.DB, queueName string) {
	for {
		rows, err := db.Query("SELECT id, task_name, data FROM queue_tasks WHERE status = 'pending' AND queue_name = ? LIMIT 1", queueName)
		if err != nil {
			logError("Erro ao buscar tarefa na fila %s (não configurada): %v", queueName, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if !rows.Next() {
			rows.Close()
			time.Sleep(5 * time.Second)
			continue
		}
		var taskID int
		var taskName, data string
		if err := rows.Scan(&taskID, &taskName, &data); err != nil {
			logError("Erro ao escanear tarefa na fila %s (não configurada): %v", queueName, err)
			rows.Close()
			continue
		}
		rows.Close()
		_, err = db.Exec("UPDATE queue_tasks SET status = 'processing' WHERE id = ?", taskID)
		if err != nil {
			db.Exec("UPDATE queue_tasks SET exception = ? WHERE id = ?", fmt.Sprintf("Erro ao atualizar para processing: %v", err), taskID)
			logError("Erro ao atualizar tarefa para 'processing' na fila %s (não configurada): %v", queueName, err)
			continue
		}
		startTime := time.Now()
		logInfo(formatJobLog(taskName, "RUNNING"))
		pluginPath := fmt.Sprintf("%s.so", taskName)
		var processingError error
		p, err := plugin.Open(pluginPath)
		if err != nil {
			processingError = fmt.Errorf("erro ao abrir plugin %s: %v", pluginPath, err)
		} else {
			symNew, err := p.Lookup("New")
			if err != nil {
				processingError = fmt.Errorf("erro ao procurar símbolo New no plugin %s: %v", pluginPath, err)
			} else {
				newFunc, ok := symNew.(func([]byte) common.Job)
				if !ok {
					processingError = fmt.Errorf("o símbolo New do plugin %s tem assinatura inválida", pluginPath)
				} else {
					job := newFunc([]byte(data))
					processingError = job.Handle()
				}
			}
		}
		elapsed := time.Since(startTime)
		if processingError != nil {
			_, err = db.Exec("UPDATE queue_tasks SET status = 'failed', exception = ? WHERE id = ?", processingError.Error(), taskID)
			if err != nil {
				logError("Erro ao atualizar tarefa para 'failed' na fila %s (não configurada): %v", queueName, err)
			}
			logError(formatJobLog(taskName, fmt.Sprintf("%s FAIL", formatDuration(elapsed))))
			// Não deleta o registro em caso de falha.
		} else {
			// Insere o registro na tabela de histórico (queue_tasks_completed)
			_, err = db.Exec(`INSERT INTO queue_tasks_completed (queue_task_id, queue_name, task_name, data, status, completed_at)
			VALUES (?, ?, ?, ?, ?, NOW())`, taskID, queueName, taskName, data, "completed")
			if err != nil {
				logError("Erro ao inserir registro em queue_tasks_completed: %v", err)
			}
			logInfo(formatJobLog(taskName, fmt.Sprintf("%s DONE", formatDuration(elapsed))))
			_, err = db.Exec("DELETE FROM queue_tasks WHERE id = ?", taskID)
			if err != nil {
				logError("Erro ao deletar tarefa %d da fila %s (não configurada): %v", taskID, queueName, err)
			}
		}
	}
}

func getDistinctNonConfiguredQueueNames(db *sql.DB, knownQueues []string) ([]string, error) {
	placeholders := strings.Repeat("?,", len(knownQueues))
	if len(knownQueues) > 0 {
		placeholders = placeholders[:len(placeholders)-1]
	}
	query := fmt.Sprintf("SELECT DISTINCT queue_name FROM queue_tasks WHERE status = 'pending' AND queue_name NOT IN (%s)", placeholders)
	args := make([]interface{}, len(knownQueues))
	for i, q := range knownQueues {
		args[i] = q
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func monitorNonConfiguredQueues(db *sql.DB, knownQueues []string) {
	spawnedQueues := make(map[string]bool)
	for {
		names, err := getDistinctNonConfiguredQueueNames(db, knownQueues)
		if err != nil {
			logError("Erro ao obter filas não configuradas: %v", err)
			time.Sleep(10 * time.Second)
			continue
		}
		for _, q := range names {
			if !spawnedQueues[q] {
				spawnedQueues[q] = true
				logInfo("Iniciando processador para a fila não configurada: %s", q)
				go processNonConfiguredQueue(db, q)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func logInfo(format string, args ...interface{}) {
	info := color.New(color.FgWhite, color.BgBlue, color.Bold).SprintfFunc()
	fmt.Printf("[ %s ] | %s | %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		info(" INFO "),
		fmt.Sprintf(format, args...))
}

func logError(format string, args ...interface{}) {
	errorMsg := color.New(color.FgWhite, color.BgRed, color.Bold).SprintfFunc()
	fmt.Printf("[ %s ] | %s | %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		errorMsg(" ERROR "),
		fmt.Sprintf(format, args...))
}

func logWarning(format string, args ...interface{}) {
	warning := color.New(color.FgBlack, color.BgYellow, color.Bold).SprintfFunc()
	fmt.Printf("[ %s ] | %s | %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		warning(" WARNING "),
		fmt.Sprintf(format, args...))
}

func logFatal(message string) {
	color.New(color.FgWhite, color.BgRed, color.Bold).
		Printf("[ %s ] | %s | %s\n",
			time.Now().Format("2006-01-02 15:04:05"),
			" FATAL ",
			message)
	os.Exit(1)
}

func main() {
	DBUser, DBPassword, DBName, DBHost, DBPort, err := loadEnvConfig()
	if err != nil {
		logFatal("Erro ao carregar configurações do banco de dados")
	}
	db, err := connectToDatabase(DBUser, DBPassword, DBName, DBHost, DBPort)
	if err != nil {
		logFatal("Erro ao conectar ao banco de dados")
	}
	defer db.Close()
	err = runMigrations(db)
	if err != nil {
		logFatal(fmt.Sprintf("Erro ao executar migrações: %v", err))
	}
	queues, err := loadQueuesConfig()
	if err != nil {
		logFatal("Erro ao carregar configuração das filas")
	}
	
	for _, queue := range queues {
		go func(q string) {
			for {
				processQueue(db, q)
				time.Sleep(5 * time.Second)
			}
		}(queue)
	}
	go monitorNonConfiguredQueues(db, queues)

	go web.Start()
	
	select {}
}
