package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"go-queue/common"
	"io/ioutil"
	"math/rand"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/go-sql-driver/mysql"
)

// -------------------------
// Contador Global para Empresas
// -------------------------
var (
	companyMessageCounters = make(map[int]int)
	counterMutex           sync.Mutex
)

// incrementCompanyCounter incrementa e retorna o contador atual para uma empresa.
func incrementCompanyCounter(companyID int) int {
	counterMutex.Lock()
	defer counterMutex.Unlock()
	companyMessageCounters[companyID]++
	fmt.Printf("[DEBUG] Contador atualizado para company_id %d: %d\n", companyID, companyMessageCounters[companyID])
	return companyMessageCounters[companyID]
}

// resetCompanyCounter reseta o contador para uma empresa.
func resetCompanyCounter(companyID int) {
	counterMutex.Lock()
	defer counterMutex.Unlock()
	companyMessageCounters[companyID] = 0
	fmt.Printf("[DEBUG] Contador resetado para company_id %d\n", companyID)
}

// -------------------------
// Função de Configuração de Marketing (Delay)
// -------------------------
// Retorna: tempoMin (mínimo de delay), tempoMax (máximo de delay),
// limite (quantidade de mensagens para disparar o delay extra) e aguardar (tempo extra em segundos).
func getMarketingConfig(companyID int) (int, int, int, int, error) {
	dsn, err := getDSN()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()
	var tempoMin, tempoMax, limite, aguardar int
	query := "SELECT tempo_minimo, tempo_maximo, mensagens_limite, aguardar_tempo FROM marketing_configs WHERE company_id = ? LIMIT 1"
	err = db.QueryRow(query, companyID).Scan(&tempoMin, &tempoMax, &limite, &aguardar)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Printf("[DEBUG] Nenhuma configuração encontrada em marketing_configs para companyID %d. Usando fallback.\n", companyID)
		} else {
			fmt.Printf("[DEBUG] Erro ao consultar marketing_configs: %v\n", err)
		}
		var rMin, rMax int
		if err2 := db.QueryRow("SELECT random_envios_minimo, random_envios_maximo FROM config_system LIMIT 1").Scan(&rMin, &rMax); err2 != nil {
			return 0, 0, 0, 0, fmt.Errorf("Erro ao obter config_system: %v", err2)
		}
		tempoMin, tempoMax, limite, aguardar = rMin, rMax, 100, 30
	}
	if tempoMax < tempoMin {
		tempoMax = tempoMin
	}
	fmt.Printf("[DEBUG] Configuração obtida: tempoMin: %d, tempoMax: %d, limite: %d, aguardar: %d\n",
		tempoMin, tempoMax, limite, aguardar)
	return tempoMin, tempoMax, limite, aguardar, nil
}

// -------------------------
// Funções Auxiliares para Mídia
// -------------------------
func getMediaTypeFromURL(mediaURL string) string {
	parsedURL, err := url.Parse(mediaURL)
	if err != nil {
		fmt.Println("Erro ao analisar a URL da mídia:", err)
		return "document"
	}
	ext := filepath.Ext(parsedURL.Path)
	if ext == "" {
		return "document"
	}
	mimeType := mime.TypeByExtension(ext)
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return "image"
	case strings.HasPrefix(mimeType, "video/"):
		return "video"
	case strings.HasPrefix(mimeType, "audio/"):
		return "audio"
	case strings.HasPrefix(mimeType, "application/pdf"):
		return "document"
	default:
		return "document"
	}
}

// -------------------------
// Funções de Acesso ao Banco
// -------------------------
func getDSN() (string, error) {
	err := godotenv.Load()
	if err != nil {
		return "", fmt.Errorf("Erro ao carregar .env: %v", err)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)
	fmt.Printf("[DEBUG] DSN obtido: %s\n", dsn)
	return dsn, nil
}

func sendMessageRecordExists(id int) (bool, error) {
	dsn, err := getDSN()
	if err != nil {
		return false, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	var count int
	query := "SELECT COUNT(*) FROM send_messages_all WHERE id = ? AND DATE(created_at) = CURDATE()"
	fmt.Printf("[DEBUG] Executando query em sendMessageRecordExists: %s com id: %d\n", query, id)
	err = db.QueryRow(query, id).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("Erro ao consultar send_messages_all: %v", err)
	}
	return count > 0, nil
}

func sendMessageRecordUserExists(id int) (bool, error) {
	dsn, err := getDSN()
	if err != nil {
		return false, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	var count int
	query := "SELECT COUNT(*) FROM sendmessageuser WHERE id = ? AND DATE(created_at) = CURDATE()"
	fmt.Printf("[DEBUG] Executando query em sendMessageRecordUserExists: %s com id: %d\n", query, id)
	err = db.QueryRow(query, id).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("Erro ao consultar sendmessageuser: %v", err)
	}
	return count > 0, nil
}

func updateSendMessageStatus(id int, status string) error {
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	query := "UPDATE send_messages_all SET status = ? WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em updateSendMessageStatus: %s para id: %d, status: %s\n", query, id, status)
	_, err = db.Exec(query, status, id)
	if err != nil {
		return fmt.Errorf("Erro ao atualizar send_messages_all: %v", err)
	}
	return nil
}

func updateUserMessageStatus(id int, status string) error {
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	query := "UPDATE sendmessageuser SET status = ? WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em updateUserMessageStatus: %s para id: %d, status: %s\n", query, id, status)
	_, err = db.Exec(query, status, id)
	if err != nil {
		return fmt.Errorf("Erro ao atualizar sendmessageuser: %v", err)
	}
	return nil
}

// getCompanyID obtém o company_id da tabela send_messages_all para o sendMessageID.
func getCompanyID(sendMessageID int) (int, error) {
	dsn, err := getDSN()
	if err != nil {
		return 0, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	var companyID int
	query := "SELECT company_id FROM send_messages_all WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em getCompanyID: %s com sendMessageID: %d\n", query, sendMessageID)
	err = db.QueryRow(query, sendMessageID).Scan(&companyID)
	if err != nil {
		fmt.Printf("[DEBUG] Erro em getCompanyID para sendMessageID %d: %v\n", sendMessageID, err)
		return 0, fmt.Errorf("Erro ao obter company_id: %v", err)
	}
	fmt.Printf("[DEBUG] company_id obtido em getCompanyID: %d\n", companyID)
	return companyID, nil
}

// getUserCompanyID obtém o company_id da tabela sendmessageuser para o sendMessageID.
func getUserCompanyID(sendMessageID int) (int, error) {
	dsn, err := getDSN()
	if err != nil {
		return 0, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	var companyID int
	query := "SELECT company_id FROM sendmessageuser WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em getUserCompanyID: %s com sendMessageID: %d\n", query, sendMessageID)
	err = db.QueryRow(query, sendMessageID).Scan(&companyID)
	if err != nil {
		fmt.Printf("[DEBUG] Erro em getUserCompanyID para sendMessageID %d: %v\n", sendMessageID, err)
		return 0, fmt.Errorf("Erro ao obter company_id: %v", err)
	}
	fmt.Printf("[DEBUG] company_id obtido em getUserCompanyID: %d\n", companyID)
	return companyID, nil
}

// -------------------------
// Estrutura do Job e Processamento
// -------------------------
type SendMessageJob struct {
	InstanceName  string `json:"instance_name"`
	Number        string `json:"number"`
	TextMessage   string `json:"text"`
	MediaURL      string `json:"media"`
	MediaType     string `json:"media_type"`
	FileName      string `json:"file_name"`
	SendMessageID int    `json:"send_message_id"`
	Client        string `json:"client"`
	AutoMessage   bool   `json:"auto_message"`
}

func (j *SendMessageJob) Handle() error {
	fmt.Printf("[DEBUG] Iniciando Handle para sendMessageID: %d\n", j.SendMessageID)

	// Verifica se o registro existe na tabela correspondente.
	if j.Client == "user" {
		exists, err := sendMessageRecordUserExists(j.SendMessageID)
		if err != nil {
			return fmt.Errorf("Erro ao verificar registro sendmessageuser: %v", err)
		}
		if !exists {
			fmt.Printf("Registro sendmessageuser com id %d não existe; cancelando envio.\n", j.SendMessageID)
			return nil
		}
	} else {
		exists, err := sendMessageRecordExists(j.SendMessageID)
		if err != nil {
			return fmt.Errorf("Erro ao verificar registro send_messages_all: %v", err)
		}
		if !exists {
			fmt.Printf("Registro send_messages_all com id %d não existe; cancelando envio.\n", j.SendMessageID)
			return nil
		}
	}

	// Obtém o company_id com base no campo Client.
	var companyID int
	var err error
	if j.Client == "user" {
		companyID, err = getUserCompanyID(j.SendMessageID)
		if err != nil {
			fmt.Printf("[DEBUG] Erro ao obter company_id da tabela sendmessageuser para sendMessageID %d: %v\n", j.SendMessageID, err)
			return err
		}
	} else {
		companyID, err = getCompanyID(j.SendMessageID)
		if err != nil {
			fmt.Printf("[DEBUG] Erro ao obter company_id da tabela send_messages_all para sendMessageID %d: %v\n", j.SendMessageID, err)
			return err
		}
	}
	fmt.Printf("[DEBUG] company_id obtido: %d para sendMessageID: %d\n", companyID, j.SendMessageID)
	if companyID == 0 {
		fmt.Printf("[DEBUG] O company_id retornado é 0 para sendMessageID %d. Verifique os dados do registro no banco.\n", j.SendMessageID)
		// Aqui você pode optar por cancelar o envio ou definir um valor padrão.
	}

	// Recupera a configuração de marketing (delay) para a empresa.
	tempoMin, tempoMax, limite, aguardar, err := getMarketingConfig(companyID)
	if err != nil {
		fmt.Printf("[DEBUG] Erro ao obter configuração de marketing para company_id %d: %v\n", companyID, err)
		tempoMin, tempoMax, limite, aguardar = 5, 5, 0, 0
	}

	if j.Client == "user" {
		if err := updateUserMessageStatus(j.SendMessageID, "sending"); err != nil {
			fmt.Printf("[DEBUG] Erro ao atualizar status na tabela sendmessageuser: %v\n", err)
		}
	} else {
		if err := updateSendMessageStatus(j.SendMessageID, "sending"); err != nil {
			fmt.Printf("[DEBUG] Erro ao atualizar status na tabela send_messages_all: %v\n", err)
		}
	}

	if j.AutoMessage {
		rand.Seed(time.Now().UnixNano())
		delaySec := rand.Intn(tempoMax-tempoMin+1) + tempoMin
		randomDelay := time.Duration(delaySec) * time.Second
		fmt.Printf("[DEBUG] AutoMessage ativado! Aguardando %v antes de enviar a mensagem...\n", randomDelay)
		time.Sleep(randomDelay)

		// Incrementa o contador global
		currentCount := incrementCompanyCounter(companyID)
		if limite > 0 && currentCount >= limite {
			fmt.Printf("[DEBUG] Limite atingido para a empresa %d. Aguardando %d segundos...\n", companyID, aguardar)
			time.Sleep(time.Duration(aguardar) * time.Second)
			resetCompanyCounter(companyID)
		}
	} else {
		fmt.Println("[DEBUG] AutoMessage desativado! Mensagem será enviada sem delay.")
	}

	// Carrega as variáveis de ambiente para a API (para esse job usamos API_KEY_3 e API_URL_3).
	err = godotenv.Load()
	if err != nil {
		fmt.Println("Erro ao carregar .env:", err)
		return err
	}
	apiKey := os.Getenv("API_KEY_3")
	serverURL := os.Getenv("API_URL_3")
	if apiKey == "" || serverURL == "" {
		return fmt.Errorf("API_KEY_3 ou API_URL_3 não estão definidos no .env")
	}

	// Monta a URL da API e o payload de acordo com se houver mídia ou não.
	var apiURL string
	var payloadData map[string]interface{}
	if j.MediaURL == "" {
		apiURL = fmt.Sprintf("%s/send-text", serverURL)
		payloadData = map[string]interface{}{
			"instance_id": j.InstanceName,
			"phone":       j.Number,
			"body":        j.TextMessage,
		}
	} else {
		if j.MediaType == "" {
			j.MediaType = getMediaTypeFromURL(j.MediaURL)
		}
		if j.FileName == "" {
			j.FileName = filepath.Base(j.MediaURL)
		}
		apiURL = fmt.Sprintf("%s/send-media", serverURL)
		payloadData = map[string]interface{}{
			"instance_id": j.InstanceName,
			"phone":       j.Number,
			"caption":     j.TextMessage,
			"media_type":  j.MediaType,
			"media":       j.MediaURL,
			"file_name":   j.FileName,
		}
	}

	payloadBytes, err := json.Marshal(payloadData)
	if err != nil {
		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao converter payload: %v", err))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao converter payload: %v", err))
		}
		return fmt.Errorf("erro ao converter payload para JSON: %v", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao criar requisição: %v", err))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao criar requisição: %v", err))
		}
		return fmt.Errorf("erro ao criar requisição: %v", err)
	}

	req.Header.Add("X-API-Key", apiKey)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao enviar requisição: %v", err))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao enviar requisição: %v", err))
		}
		return fmt.Errorf("erro ao enviar requisição: %v", err)
	}
	defer res.Body.Close()

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao ler resposta da API: %v", err))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao ler resposta da API: %v", err))
		}
		return fmt.Errorf("erro ao ler resposta da API: %v", err)
	}

	// Decodifica a resposta da API
	var apiResponse struct {
		Message       string                 `json:"message"`
		Status        int                    `json:"status"`
		StatusMessage string                 `json:"statusMessage"`
		Success       bool                   `json:"success"`
		Data          map[string]interface{} `json:"data"`
	}
	err = json.Unmarshal(bodyBytes, &apiResponse)
	if err != nil {
		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao decodificar resposta: %v", err))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao decodificar resposta: %v", err))
		}
		return fmt.Errorf("erro ao decodificar resposta da API: %v", err)
	}

	var newStatus string
	if apiResponse.Success {
		newStatus = "sent"
	} else {
		if apiResponse.Status == 404 {
			newStatus = "Whatsapp não existe"
		} else {
			newStatus = "Erro desconhecido"
		}
	}

	if j.Client == "user" {
		if err := updateUserMessageStatus(j.SendMessageID, newStatus); err != nil {
			fmt.Printf("Erro ao atualizar status na tabela sendmessageuser: %v\n", err)
		}
	} else {
		if err := updateSendMessageStatus(j.SendMessageID, newStatus); err != nil {
			fmt.Printf("Erro ao atualizar status na tabela send_messages_all: %v\n", err)
		}
	}

	fmt.Printf("[DEBUG] Resposta da API: %s\n", string(bodyBytes))
	return nil
}

// New cria o job a partir dos dados JSON.
func New(data []byte) common.Job {
	var job SendMessageJob
	err := json.Unmarshal(data, &job)
	if err != nil {
		fmt.Printf("Erro ao decodificar JSON: %v\n", err)
		return nil
	}
	return &job
}
