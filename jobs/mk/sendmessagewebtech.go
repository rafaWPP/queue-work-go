package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"go-queue/common" // Seu pacote que define a interface Job
	"io/ioutil"
	"math/rand"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/go-sql-driver/mysql"
)

// -----------------------------------------------------------------------------
// STRUCT PRINCIPAL DO JOB
// -----------------------------------------------------------------------------

// ProcessCampaignJob representa um job para enviar uma única mensagem de uma campanha
// usando API_KEY_3 / API_URL_3.
type ProcessCampaignJob struct {
	CampaignID     int    `json:"campaign_id"`      // ID da campanha
	InstanceName   string `json:"instance_name"`    // Nome/ID da instância do WhatsApp
	Text           string `json:"text"`             // Texto final (tags/spintax já resolvidos) recebido do Laravel
	MediaURL       string `json:"media_url"`        // URL da mídia (se houver) recebida do Laravel
	Phone          string `json:"phone"`            // Telefone informado no payload
	CampaignSendID int    `json:"campaign_send_id"` // ID do registro de envio para atualizar status
}

// New cria o job a partir dos dados JSON recebidos.
func New(data []byte) common.Job {
	var job ProcessCampaignJob
	if err := json.Unmarshal(data, &job); err != nil {
		fmt.Println("Erro ao decodificar JSON do Job:", err)
		return nil
	}
	return &job
}

// Handle executa o processamento do job.
// Cada job envia uma única mensagem para o telefone especificado e atualiza o status do envio.
func (j *ProcessCampaignJob) Handle() error {
	// Atualiza o status do registro para "enviando"
	updateCampaignSendStatus(j.CampaignSendID, "enviando")

	// 1) Carrega informações da campanha (status, messages_sent, company_id)
	campStatus, messagesSent, companyID, err := getCampaignByID(j.CampaignID)
	if err != nil {
		fmt.Printf("Campanha %d não encontrada: %v\n", j.CampaignID, err)
		return nil
	}
	if campStatus == "cancelado" || campStatus == "Enviado" {
		fmt.Printf("Campanha %d está em status %s; nada a fazer.\n", j.CampaignID, campStatus)
		return nil
	}

	// Se a campanha não estiver "enviando" ou "pendente", atualiza o status para "enviando"
	if campStatus != "enviando" && campStatus != "Enviado" {
		updateCampaignStatus(j.CampaignID, "enviando")
	}

	// 2) Obtém configurações de delay e limite (marketing config)
	tempoMin, tempoMax, limite, aguardar, err := getMarketingConfig(companyID)
	if err != nil {
		tempoMin, tempoMax, limite, aguardar = 10, 15, 100, 30
	}

	// 3) Aplica delay conforme as configurações
	if messagesSent >= limite {
		fmt.Printf("Limite de %d mensagens atingido; aguardando %d segundos...\n", limite, aguardar)
		time.Sleep(time.Duration(aguardar) * time.Second)
		resetMessagesSent(j.CampaignID)
		messagesSent = 0
	} else {
		delaySec := rand.Intn(tempoMax-tempoMin+1) + tempoMin
		fmt.Printf("Aguardando %d segundos antes de enviar...\n", delaySec)
		time.Sleep(time.Duration(delaySec) * time.Second)
	}

	// 4) Verifica se o telefone foi informado no payload
	if j.Phone == "" {
		fmt.Println("Telefone vazio no payload; abortando envio.")
		if j.CampaignSendID > 0 {
			updateCampaignSendStatus(j.CampaignSendID, "Sem WhatsApp")
		}
		return nil
	}

	// 5) Envia a mensagem via API usando API_KEY_3 / API_URL_3
	errSend := sendMessageAPI3(j.InstanceName, j.Phone, j.Text, j.MediaURL)
	if errSend != nil {
		if j.CampaignSendID > 0 {
			if strings.Contains(errSend.Error(), "whatsapp não existe") {
				updateCampaignSendStatus(j.CampaignSendID, "WhatsApp inexistente")
			} else {
				updateCampaignSendStatus(j.CampaignSendID, "erro")
			}
		}
		fmt.Printf("Erro ao enviar para %s: %v\n", j.Phone, errSend)
		return nil
	}

	// 6) Mensagem enviada com sucesso: atualiza o status do envio e o contador
	if j.CampaignSendID > 0 {
		updateCampaignSendStatus(j.CampaignSendID, "Enviado")
	}
	fmt.Printf("Mensagem enviada com sucesso para %s\n", j.Phone)
	incrementMessagesSent(j.CampaignID)

	// (Opcional) Verifica se ainda há envios pendentes para atualizar o status da campanha
	restantes, _ := getCampaignSendsPendentes(j.CampaignID)
	if len(restantes) == 0 {
		updateCampaignStatus(j.CampaignID, "enviado")
		resetMessagesSent(j.CampaignID)
		fmt.Printf("Campanha %d finalizada.\n", j.CampaignID)
		return nil
	} else {
		updateCampaignStatus(j.CampaignID, "enviando")
		fmt.Printf("Campanha %d ainda tem %d pendentes.\n", j.CampaignID, len(restantes))
	}

	return nil
}

//
// --- Funções auxiliares de banco e configuração ---
//

func getDSN() (string, error) {
	if err := godotenv.Load(); err != nil {
		return "", fmt.Errorf("Erro ao carregar .env: %v", err)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"))
	return dsn, nil
}

func getCampaignByID(campaignID int) (string, int, int, error) {
	dsn, err := getDSN()
	if err != nil {
		return "", 0, 0, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", 0, 0, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	var status string
	var messagesSent, companyID int
	query := "SELECT status, messages_sent, company_id FROM campaigns WHERE id = ? LIMIT 1"
	err = db.QueryRow(query, campaignID).Scan(&status, &messagesSent, &companyID)
	if err != nil {
		return "", 0, 0, err
	}
	return status, messagesSent, companyID, nil
}

func updateCampaignStatus(campaignID int, newStatus string) error {
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("UPDATE campaigns SET status = ? WHERE id = ?", newStatus, campaignID)
	return err
}

func incrementMessagesSent(campaignID int) error {
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("UPDATE campaigns SET messages_sent = messages_sent + 1 WHERE id = ?", campaignID)
	return err
}

func resetMessagesSent(campaignID int) error {
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("UPDATE campaigns SET messages_sent = 0 WHERE id = ?", campaignID)
	return err
}

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
		// Fallback para config_system se não encontrar marketing_configs
		var rMin, rMax int
		if err2 := db.QueryRow("SELECT random_envios_minimo, random_envios_maximo FROM config_system LIMIT 1").Scan(&rMin, &rMax); err2 != nil {
			return 0, 0, 0, 0, fmt.Errorf("Erro ao obter config_system: %v", err2)
		}
		tempoMin, tempoMax, limite, aguardar = rMin, rMax, 100, 30
	}
	return tempoMin, tempoMax, limite, aguardar, nil
}

func getCampaignSendsPendentes(campaignID int) ([]CampaignSend, error) {
	dsn, err := getDSN()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	query := `
		SELECT 
			id, 
			status
		FROM campaign_sends
		WHERE campaign_id = ?
		  AND status = 'pendente'
	`
	rows, err := db.Query(query, campaignID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []CampaignSend
	for rows.Next() {
		var cs CampaignSend
		if err := rows.Scan(&cs.ID, &cs.Status); err != nil {
			return nil, err
		}
		result = append(result, cs)
	}
	return result, nil
}

// CampaignSend representa um envio pendente da campanha (apenas ID e status são necessários).
type CampaignSend struct {
	ID     int
	Status string
}

func updateCampaignSendStatus(sendID int, newStatus string) error {
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()
	_, err = db.Exec("UPDATE campaign_sends SET status = ? WHERE id = ?", newStatus, sendID)
	return err
}

//
// --- ENVIO DA MENSAGEM VIA API (API_KEY_3 / API_URL_3) ---
//

func sendMessageAPI3(instanceName, number, text, media string) error {
	if err := godotenv.Load(); err != nil {
		return fmt.Errorf("Erro ao carregar .env: %v", err)
	}
	apiKey := os.Getenv("API_KEY_3")   // Usamos API_KEY_3
	serverURL := os.Getenv("API_URL_3") // e API_URL_3
	if apiKey == "" || serverURL == "" {
		return fmt.Errorf("API_KEY_3 ou API_URL_3 não definidos no .env")
	}

	var apiURL string
	var payloadData map[string]interface{}

	// Se não há mídia, usamos o endpoint de texto
	if media == "" {
		apiURL = fmt.Sprintf("%s/send-text", serverURL)
		payloadData = map[string]interface{}{
			"instance_id": instanceName,
			"phone":       number,
			"body":        text,
		}
	} else {
		// Com mídia, usamos o endpoint de mídia
		mediaType := getMediaTypeFromURL(media)
		fileName := filepath.Base(media)
		if getMimeTypeFromURLReal(media) == "application/pdf" && !strings.HasSuffix(strings.ToLower(fileName), ".pdf") {
			fileName += ".pdf"
		}
		apiURL = fmt.Sprintf("%s/send-media", serverURL)
		payloadData = map[string]interface{}{
			"instance_id": instanceName,
			"phone":       number,
			"media_type":  mediaType,
			"file_name":   fileName,
			"caption":     text,
			"media":       media,
		}
	}

	payloadBytes, err := json.Marshal(payloadData)
	if err != nil {
		return fmt.Errorf("Erro ao converter payload JSON: %v", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("Erro ao criar requisição: %v", err)
	}
	req.Header.Add("X-API-Key", apiKey)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Erro ao enviar requisição: %v", err)
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Erro ao ler resposta da API: %v", err)
	}

	// Decodifica a resposta da API
	var apiResponse struct {
		Message string                 `json:"message"`
		Status  int                    `json:"status"`
		Success bool                   `json:"success"`
		Data    map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return fmt.Errorf("Erro ao decodificar resposta da API: %v", err)
	}

	// Se o envio foi bem-sucedido, retorna nil
	if apiResponse.Success {
		fmt.Println("Mensagem enviada com sucesso:", string(body))
		return nil
	}

	// Se o status for 404, trata como "WhatsApp não existe"
	if apiResponse.Status == 404 {
		return fmt.Errorf("whatsapp não existe (status=404). Resposta: %s", string(body))
	}
	return fmt.Errorf("Erro desconhecido. Resposta: %s", string(body))
}

//
// --- Funções para detectar tipo de mídia ---
//

func getMediaTypeFromURL(mediaURL string) string {
	parsedURL, err := url.Parse(mediaURL)
	if err != nil {
		fmt.Println("Erro ao analisar URL da mídia:", err)
		return "document"
	}
	ext := filepath.Ext(parsedURL.Path)
	if ext == "" {
		return "document"
	}
	mType := mime.TypeByExtension(ext)
	switch {
	case strings.HasPrefix(mType, "image/"):
		return "image"
	case strings.HasPrefix(mType, "video/"):
		return "video"
	case strings.HasPrefix(mType, "audio/"):
		return "audio"
	case strings.HasPrefix(mType, "application/pdf"):
		return "document"
	default:
		return "document"
	}
}

func getMimeTypeFromURLReal(mediaURL string) string {
	parsedURL, err := url.Parse(mediaURL)
	if err != nil {
		fmt.Println("Erro ao analisar URL da mídia:", err)
		return "application/octet-stream"
	}
	ext := strings.ToLower(filepath.Ext(parsedURL.Path))
	if ext == "" {
		return "application/octet-stream"
	}
	if ext == ".pdf" {
		return "application/pdf"
	}
	mt := mime.TypeByExtension(ext)
	if mt == "" {
		return "application/octet-stream"
	}
	return mt
}
