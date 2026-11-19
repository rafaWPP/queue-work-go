package common

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
)

// LogJobError registra erros no banco de dados e no arquivo de log.
func LogJobError(db *sql.DB, taskID int, taskName string, err error) {
	if err == nil {
		return
	}

	errorMessage := fmt.Sprintf("[%s] ERRO NO JOB: %s | %s",
		time.Now().Format("2006-01-02 15:04:05"), taskName, err.Error())

	// Log no console
	log.Println(errorMessage)

	// Salvar erro no arquivo de log
	file, fileErr := os.OpenFile("errors.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if fileErr != nil {
		log.Println("Erro ao abrir arquivo de erro:", fileErr)
		return
	}
	defer file.Close()

	_, fileErr = file.WriteString(errorMessage + "\n")
	if fileErr != nil {
		log.Println("Erro ao escrever no arquivo de erro:", fileErr)
	}

	// Atualizar o status no banco de dados
	_, dbErr := db.Exec("UPDATE queue_tasks SET status = 'failed', exception = ? WHERE id = ?", err.Error(), taskID)
	if dbErr != nil {
		log.Println("Erro ao atualizar status de falha no banco:", dbErr)
	}
}

// LogWorkerSyncIssue registra inconsistências entre o worker e as tabelas
// de rastreio de envio para investigação posterior.
func LogWorkerSyncIssue(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	entry := fmt.Sprintf("[%s] %s", time.Now().Format("2006-01-02 15:04:05"), message)

	log.Println(entry)

	file, err := os.OpenFile("worker_sync_errors.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Erro ao abrir worker_sync_errors.log:", err)
		return
	}
	defer file.Close()

	if _, err := file.WriteString(entry + "\n"); err != nil {
		log.Println("Erro ao escrever em worker_sync_errors.log:", err)
	}
}
