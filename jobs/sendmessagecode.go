package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go-queue/common"
	"io/ioutil"
	"math/rand"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
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
// Função de Configuração de Marketing
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
		fmt.Printf("Erro ao analisar a URL da mídia: %v\n", err)
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
		fmt.Printf("Erro ao analisar a URL da mídia: %v\n", err)
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
	query := "SELECT COUNT(*) FROM send_messages_all WHERE id = ?"
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
	query := "SELECT COUNT(*) FROM sendmessageuser WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em sendMessageRecordUserExists: %s com id: %d\n", query, id)
	err = db.QueryRow(query, id).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("Erro ao consultar sendmessageuser: %v", err)
	}
	return count > 0, nil
}

func updateSendMessageStatus(id int, status string) error {
	status = normalizeStatusForDB(status)
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	query := "UPDATE send_messages_all SET status = ?, updated_at = NOW() WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em updateSendMessageStatus: %s para id: %d, status: %s\n", query, id, status)
	_, err = db.Exec(query, status, id)
	if err != nil {
		return fmt.Errorf("Erro ao atualizar send_messages_all: %v", err)
	}
	return nil
}

func updateSendMessageMetaTracking(id int, metaMessageID string, metaStatus string, rawPayload string) error {
	metaStatus = normalizeStatusForDB(metaStatus)
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	query := "UPDATE send_messages_all SET meta_message_id = ?, meta_delivery_status = ?, meta_status_payload = ?, meta_status_at = NOW(), updated_at = NOW() WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em updateSendMessageMetaTracking para id: %d, meta_message_id: %s, meta_status: %s\n", id, metaMessageID, metaStatus)
	_, err = db.Exec(query, metaMessageID, metaStatus, rawPayload, id)
	if err != nil {
		return fmt.Errorf("Erro ao atualizar rastreio Meta em send_messages_all: %v", err)
	}
	return nil
}

func updateUserMessageStatus(id int, status string) error {
	status = normalizeStatusForDB(status)
	dsn, err := getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	query := "UPDATE sendmessageuser SET status = ?, updated_at = NOW() WHERE id = ?"
	fmt.Printf("[DEBUG] Executando query em updateUserMessageStatus: %s para id: %d, status: %s\n", query, id, status)
	_, err = db.Exec(query, status, id)
	if err != nil {
		return fmt.Errorf("Erro ao atualizar sendmessageuser: %v", err)
	}
	return nil
}

func normalizeStatusForDB(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return "Erro desconhecido"
	}

	runes := []rune(status)
	if len(runes) > 250 {
		return string(runes[:247]) + "..."
	}

	return status
}

func persistMetaTemplateFailure(job *SendMessageJob, err error, rawPayload string) {
	if err == nil {
		return
	}

	status := normalizeStatusForDB("Meta: " + err.Error())
	if rawPayload == "" {
		rawPayload = err.Error()
	}

	if job.Client == "user" {
		if updateErr := updateUserMessageStatus(job.SendMessageID, status); updateErr != nil {
			common.LogWorkerSyncIssue("sendmessagecode: falha ao salvar erro Meta em sendmessageuser id=%d: %v", job.SendMessageID, updateErr)
		}
		return
	}

	if updateErr := updateSendMessageMetaTracking(job.SendMessageID, "", "failed", rawPayload); updateErr != nil {
		common.LogWorkerSyncIssue("sendmessagecode: falha ao salvar rastreio Meta com erro em send_messages_all id=%d: %v", job.SendMessageID, updateErr)
	}
	if updateErr := updateSendMessageStatus(job.SendMessageID, status); updateErr != nil {
		common.LogWorkerSyncIssue("sendmessagecode: falha ao salvar erro Meta em send_messages_all id=%d: %v", job.SendMessageID, updateErr)
	}
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
	InstanceName             string            `json:"instance_name"`
	Number                   string            `json:"number"`
	TextMessage              string            `json:"text"`
	MediaURL                 string            `json:"media"`
	MediaType                string            `json:"media_type"`
	FileName                 string            `json:"file_name"`
	SendMessageID            int               `json:"send_message_id"`
	Client                   string            `json:"client"`
	AutoMessage              bool              `json:"auto_message"`
	MetaTemplate             bool              `json:"meta_template"`
	TemplateName             string            `json:"template_name"`
	TemplateLanguage         string            `json:"template_language"`
	TemplateParameters       []string          `json:"template_parameters"`
	TemplateButtonParameters map[string]string `json:"template_button_parameters"`
	TemplateButtonTypes      map[string]string `json:"template_button_types"`
	AutoReplyMessage         string            `json:"auto_reply_message"`
	AutoReplyMediaURL        string            `json:"auto_reply_media_url"`
	HeaderFormat             string            `json:"header_format"`
	HeaderMediaURL           string            `json:"header_media_url"`
}

type MetaConfig struct {
	AccessToken   string
	PhoneNumberID string
}

type MetaSendResponse struct {
	Messages []struct {
		ID string `json:"id"`
	} `json:"messages"`
}

func getMetaConfig(companyID int) (*MetaConfig, error) {
	dsn, err := getDSN()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("Erro ao conectar ao banco: %v", err)
	}
	defer db.Close()

	var accessToken, phoneNumberID string
	query := "SELECT access_token, phone_number_id FROM meta_waba_connections WHERE company_id = ? LIMIT 1"
	err = db.QueryRow(query, companyID).Scan(&accessToken, &phoneNumberID)
	if err != nil {
		return nil, err
	}
	if accessToken == "" || phoneNumberID == "" {
		return nil, fmt.Errorf("Dados de Meta incompletos")
	}
	return &MetaConfig{
		AccessToken:   accessToken,
		PhoneNumberID: phoneNumberID,
	}, nil
}

func sendMetaTemplate(job *SendMessageJob, companyID int) (bool, string, string, error) {
	if !job.MetaTemplate || job.TemplateName == "" {
		return false, "", "", nil
	}
	config, err := getMetaConfig(companyID)
	if err != nil {
		return false, "", "", err
	}
	language := job.TemplateLanguage
	if language == "" {
		language = "pt_BR"
	}

	payload := map[string]interface{}{
		"messaging_product": "whatsapp",
		"to":                job.Number,
		"type":              "template",
		"template": map[string]interface{}{
			"name": job.TemplateName,
			"language": map[string]string{
				"code": language,
			},
		},
	}

	components := make([]map[string]interface{}, 0)
	if job.HeaderFormat != "" && job.HeaderMediaURL != "" {
		headerType := strings.ToUpper(job.HeaderFormat)
		switch headerType {
		case "IMAGE":
			components = append(components, map[string]interface{}{
				"type": "header",
				"parameters": []map[string]interface{}{
					{
						"type":  "image",
						"image": map[string]string{"link": job.HeaderMediaURL},
					},
				},
			})
		case "VIDEO":
			components = append(components, map[string]interface{}{
				"type": "header",
				"parameters": []map[string]interface{}{
					{
						"type":  "video",
						"video": map[string]string{"link": job.HeaderMediaURL},
					},
				},
			})
		case "DOCUMENT":
			components = append(components, map[string]interface{}{
				"type": "header",
				"parameters": []map[string]interface{}{
					{
						"type":     "document",
						"document": map[string]string{"link": job.HeaderMediaURL},
					},
				},
			})
		}
	}

	if len(job.TemplateParameters) > 0 {
		parameters := make([]map[string]string, 0, len(job.TemplateParameters))
		for _, value := range job.TemplateParameters {
			parameters = append(parameters, map[string]string{
				"type": "text",
				"text": value,
			})
		}
		components = append(components, map[string]interface{}{
			"type":       "body",
			"parameters": parameters,
		})
	}

	if len(job.TemplateButtonParameters) > 0 {
		indexes := make([]int, 0, len(job.TemplateButtonParameters))
		for key := range job.TemplateButtonParameters {
			index, err := strconv.Atoi(key)
			if err != nil {
				continue
			}
			indexes = append(indexes, index)
		}
		sort.Ints(indexes)
		for _, index := range indexes {
			value := job.TemplateButtonParameters[strconv.Itoa(index)]
			if value == "" {
				continue
			}
			buttonType := "url"
			if job.TemplateButtonTypes != nil {
				if t, ok := job.TemplateButtonTypes[strconv.Itoa(index)]; ok && t != "" {
					buttonType = strings.ToLower(t)
				}
			}
			if buttonType == "quick_reply" {
				components = append(components, map[string]interface{}{
					"type":     "button",
					"sub_type": "quick_reply",
					"index":    strconv.Itoa(index),
					"parameters": []map[string]string{
						{
							"type":    "payload",
							"payload": value,
						},
					},
				})
				continue
			}
			if buttonType == "copy_code" {
				components = append(components, map[string]interface{}{
					"type":     "button",
					"sub_type": "copy_code",
					"index":    strconv.Itoa(index),
					"parameters": []map[string]string{
						{
							"type":        "coupon_code",
							"coupon_code": value,
						},
					},
				})
				continue
			}
			components = append(components, map[string]interface{}{
				"type":     "button",
				"sub_type": "url",
				"index":    strconv.Itoa(index),
				"parameters": []map[string]string{
					{
						"type": "text",
						"text": value,
					},
				},
			})
		}
	}

	if len(components) > 0 {
		template := payload["template"].(map[string]interface{})
		template["components"] = components
	}

	graphBase := os.Getenv("META_GRAPH_BASE")
	if graphBase == "" {
		graphBase = "https://graph.facebook.com"
	}
	graphVersion := os.Getenv("META_GRAPH_VERSION")
	if graphVersion == "" {
		graphVersion = "v18.0"
	}
	apiURL := fmt.Sprintf("%s/%s/%s/messages", graphBase, graphVersion, config.PhoneNumberID)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return false, "", "", fmt.Errorf("erro ao converter payload Meta: %v", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return false, "", "", fmt.Errorf("erro ao criar requisição Meta: %v", err)
	}
	req.Header.Add("Authorization", "Bearer "+config.AccessToken)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return false, "", "", fmt.Errorf("erro ao enviar requisição Meta: %v", err)
	}
	defer res.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(res.Body)
	rawBody := string(bodyBytes)

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return false, "", rawBody, fmt.Errorf("erro Meta status %d: %s", res.StatusCode, rawBody)
	}

	metaMessageID := ""
	var metaResponse MetaSendResponse
	if err := json.Unmarshal(bodyBytes, &metaResponse); err == nil && len(metaResponse.Messages) > 0 {
		metaMessageID = metaResponse.Messages[0].ID
	}

	// Auto reply (texto + mídia) só é enviado quando o contato RESPONDER (via webhook Meta)
	return true, metaMessageID, rawBody, nil
}

func sendMetaDocumentMessage(config *MetaConfig, phone string, documentURL string) (bool, error) {
	if config == nil || config.AccessToken == "" || config.PhoneNumberID == "" {
		return false, fmt.Errorf("Dados de Meta incompletos")
	}
	if strings.TrimSpace(phone) == "" || strings.TrimSpace(documentURL) == "" {
		return false, fmt.Errorf("Dados inválidos para envio de documento")
	}

	parsedURL, _ := url.Parse(documentURL)
	filename := filepath.Base(parsedURL.Path)
	if filename == "" || filename == "." {
		filename = "documento.pdf"
	}

	payload := map[string]interface{}{
		"messaging_product": "whatsapp",
		"to":                phone,
		"type":              "document",
		"document": map[string]interface{}{
			"link":     documentURL,
			"filename": filename,
		},
	}

	graphBase := os.Getenv("META_GRAPH_BASE")
	if graphBase == "" {
		graphBase = "https://graph.facebook.com"
	}
	graphVersion := os.Getenv("META_GRAPH_VERSION")
	if graphVersion == "" {
		graphVersion = "v18.0"
	}
	apiURL := fmt.Sprintf("%s/%s/%s/messages", graphBase, graphVersion, config.PhoneNumberID)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("erro ao converter payload documento: %v", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return false, fmt.Errorf("erro ao criar requisição documento: %v", err)
	}
	req.Header.Add("Authorization", "Bearer "+config.AccessToken)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("erro ao enviar documento: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		bodyBytes, _ := ioutil.ReadAll(res.Body)
		return false, fmt.Errorf("erro documento status %d: %s", res.StatusCode, string(bodyBytes))
	}

	return true, nil
}

func (j *SendMessageJob) Handle() error {
	fmt.Printf("[DEBUG] Iniciando Handle para sendMessageID: %d\n", j.SendMessageID)

	// Verifica se o registro existe na tabela apropriada.
	if j.Client == "user" {
		exists, err := sendMessageRecordUserExists(j.SendMessageID)
		if err != nil {
			return fmt.Errorf("Erro ao verificar registro sendmessageuser: %v", err)
		}
		if !exists {
			fmt.Printf("Registro sendmessageuser com id %d não existe; cancelando envio.\n", j.SendMessageID)
			common.LogWorkerSyncIssue("sendmessagecode: registro sendmessageuser id=%d não existe; envio cancelado", j.SendMessageID)
			return nil
		}
	} else {
		exists, err := sendMessageRecordExists(j.SendMessageID)
		if err != nil {
			return fmt.Errorf("Erro ao verificar registro send_messages_all: %v", err)
		}
		if !exists {
			fmt.Printf("Registro send_messages_all com id %d não existe; cancelando envio.\n", j.SendMessageID)
			common.LogWorkerSyncIssue("sendmessagecode: registro send_messages_all id=%d não existe; envio cancelado", j.SendMessageID)
			return nil
		}
	}

	// Obtém o company_id de acordo com o campo Client.
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

	if j.Client == "user" {
		if err := updateUserMessageStatus(j.SendMessageID, "sending"); err != nil {
			fmt.Printf("[DEBUG] Erro ao atualizar status na tabela sendmessageuser: %v\n", err)
			common.LogWorkerSyncIssue("sendmessagecode: falha ao marcar sendmessageuser id=%d como sending: %v", j.SendMessageID, err)
		}
	} else {
		if err := updateSendMessageStatus(j.SendMessageID, "sending"); err != nil {
			fmt.Printf("[DEBUG] Erro ao atualizar status na tabela send_messages_all: %v\n", err)
			common.LogWorkerSyncIssue("sendmessagecode: falha ao marcar send_messages_all id=%d como sending: %v", j.SendMessageID, err)
		}
	}

	if j.AutoMessage {
		// Recupera a configuração de marketing para delay.
		tempoMin, tempoMax, limite, aguardar, err := getMarketingConfig(companyID)
		if err != nil {
			fmt.Printf("[DEBUG] Erro ao obter configuração de marketing para company_id %d: %v\n", companyID, err)
			tempoMin, tempoMax, limite, aguardar = 5, 5, 0, 0
		}
		if tempoMax < tempoMin {
			tempoMax = tempoMin
		}

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

	if j.MetaTemplate {
		sent, metaMessageID, metaPayload, err := sendMetaTemplate(j, companyID)
		if err != nil {
			fmt.Printf("[DEBUG] Falha ao enviar template Meta: %v\n", err)
			if strings.TrimSpace(j.TextMessage) == "" && strings.TrimSpace(j.MediaURL) == "" {
				persistMetaTemplateFailure(j, err, metaPayload)
				return nil
			}
		}
		if sent {
			if j.Client == "user" {
				updateUserMessageStatus(j.SendMessageID, "sent")
			} else {
				if err := updateSendMessageMetaTracking(j.SendMessageID, metaMessageID, "sent", metaPayload); err != nil {
					common.LogWorkerSyncIssue("sendmessagecode: falha ao salvar rastreio Meta em send_messages_all id=%d: %v", j.SendMessageID, err)
				}
				updateSendMessageStatus(j.SendMessageID, "sent")
			}
			return nil
		}
	}

	// Carrega as variáveis de ambiente para a API (neste job usamos API_KEY_2 e API_URL_2).
	err = godotenv.Load()
	if err != nil {
		fmt.Printf("[DEBUG] Erro ao carregar .env: %v\n", err)
		return err
	}
	apiKey := os.Getenv("API_KEY_2")
	serverURL := os.Getenv("API_URL_2")
	if apiKey == "" || serverURL == "" {
		return fmt.Errorf("API_KEY_2 ou API_URL_2 não estão definidos no .env")
	}

	// Monta a URL da API e o payload conforme se houver mídia ou não.
	var apiURL string
	var payloadData map[string]interface{}
	if j.MediaURL == "" {
		apiURL = fmt.Sprintf("%s/message/sendText/%s", serverURL, j.InstanceName)
		payloadData = map[string]interface{}{
			"number": j.Number,
			"options": map[string]interface{}{
				"delay":    100,
				"presence": "composing",
			},
			"textMessage": map[string]string{
				"text": j.TextMessage,
			},
		}
	} else {
		if j.MediaType == "" {
			j.MediaType = getMediaTypeFromURL(j.MediaURL)
		}
		if j.FileName == "" {
			j.FileName = filepath.Base(j.MediaURL)
		}
		apiURL = fmt.Sprintf("%s/message/sendMedia/%s", serverURL, j.InstanceName)
		payloadData = map[string]interface{}{
			"number": j.Number,
			"options": map[string]interface{}{
				"delay":    100,
				"presence": "composing",
			},
			"mediaMessage": map[string]interface{}{
				"mediatype": j.MediaType,
				"fileName":  j.FileName,
				"caption":   j.TextMessage,
				"media":     j.MediaURL,
				"mimeType":  getMimeTypeFromURLReal(j.MediaURL),
			},
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
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao criar requisição"))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao criar requisição"))
		}
		return fmt.Errorf("erro ao criar requisição: %v", err)
	}

	req.Header.Add("apikey", apiKey)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		var userFriendly string
		if errors.Is(err, context.DeadlineExceeded) {
			userFriendly = "Falha: tempo de resposta excedido."
		} else {
			userFriendly = "Falha ao enviar mensagem. Tente novamente mais tarde."
		}

		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, userFriendly)
		} else {
			updateSendMessageStatus(j.SendMessageID, userFriendly)
		}
		return fmt.Errorf("erro ao enviar requisição: %v", err)
	}
	defer res.Body.Close()

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao ler resposta da API"))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao ler resposta da API"))
		}
		return fmt.Errorf("erro ao ler resposta da API: %v", err)
	}

	var apiResponse map[string]interface{}
	err = json.Unmarshal(bodyBytes, &apiResponse)
	if err != nil {
		if j.Client == "user" {
			updateUserMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao decodificar resposta: %v", err))
		} else {
			updateSendMessageStatus(j.SendMessageID, fmt.Sprintf("Erro ao decodificar resposta: %v", err))
		}
		return fmt.Errorf("erro ao decodificar resposta da API: %v", err)
	}

	extractStatusMessage := func(value interface{}) string {
		switch typed := value.(type) {
		case string:
			return strings.TrimSpace(typed)
		case []interface{}:
			for _, item := range typed {
				if asMap, ok := item.(map[string]interface{}); ok {
					if message, ok := asMap["message"].(string); ok && strings.TrimSpace(message) != "" {
						return strings.TrimSpace(message)
					}
					if errorMessage, ok := asMap["error"].(string); ok && strings.TrimSpace(errorMessage) != "" {
						return strings.TrimSpace(errorMessage)
					}
				}
			}
			if payload, marshalErr := json.Marshal(typed); marshalErr == nil {
				return strings.TrimSpace(string(payload))
			}
		case map[string]interface{}:
			if message, ok := typed["message"].(string); ok && strings.TrimSpace(message) != "" {
				return strings.TrimSpace(message)
			}
			if errorMessage, ok := typed["error"].(string); ok && strings.TrimSpace(errorMessage) != "" {
				return strings.TrimSpace(errorMessage)
			}
			if payload, marshalErr := json.Marshal(typed); marshalErr == nil {
				return strings.TrimSpace(string(payload))
			}
		}
		return ""
	}

	normalizedMessage := extractStatusMessage(apiResponse["message"])
	if normalizedMessage == "" {
		normalizedMessage = extractStatusMessage(apiResponse["error"])
	}

	var newStatus string
	if statusVal, ok := apiResponse["status"]; ok {
		if statusStr, ok := statusVal.(string); ok && statusStr == "PENDING" {
			newStatus = "sent"
		} else if num, ok := statusVal.(float64); ok && int(num) == 400 {
			if normalizedMessage != "" {
				newStatus = normalizedMessage
			} else {
				newStatus = "Whatsapp não existe"
			}
		} else if normalizedMessage != "" {
			newStatus = normalizedMessage
		} else {
			newStatus = "Erro desconhecido"
		}
	} else if normalizedMessage != "" {
		newStatus = normalizedMessage
	} else {
		newStatus = "Erro desconhecido"
	}

	if j.Client == "user" {
		if err := updateUserMessageStatus(j.SendMessageID, newStatus); err != nil {
			fmt.Printf("Erro ao atualizar status na tabela sendmessageuser: %v\n", err)
			common.LogWorkerSyncIssue("sendmessagecode: falha ao marcar sendmessageuser id=%d como %s: %v", j.SendMessageID, newStatus, err)
		}
	} else {
		if err := updateSendMessageStatus(j.SendMessageID, newStatus); err != nil {
			fmt.Printf("Erro ao atualizar status na tabela send_messages_all: %v\n", err)
			common.LogWorkerSyncIssue("sendmessagecode: falha ao marcar send_messages_all id=%d como %s: %v", j.SendMessageID, newStatus, err)
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

func sendMetaTextMessage(config *MetaConfig, phone string, message string) (bool, error) {
	if config == nil || config.AccessToken == "" || config.PhoneNumberID == "" {
		return false, fmt.Errorf("Dados de Meta incompletos")
	}
	if strings.TrimSpace(phone) == "" || strings.TrimSpace(message) == "" {
		return false, fmt.Errorf("Dados inválidos para mensagem de texto")
	}

	payload := map[string]interface{}{
		"messaging_product": "whatsapp",
		"to":                phone,
		"type":              "text",
		"text": map[string]interface{}{
			"body":        message,
			"preview_url": false,
		},
	}

	graphBase := os.Getenv("META_GRAPH_BASE")
	if graphBase == "" {
		graphBase = "https://graph.facebook.com"
	}
	graphVersion := os.Getenv("META_GRAPH_VERSION")
	if graphVersion == "" {
		graphVersion = "v18.0"
	}
	apiURL := fmt.Sprintf("%s/%s/%s/messages", graphBase, graphVersion, config.PhoneNumberID)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("erro ao converter payload Meta: %v", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return false, fmt.Errorf("erro ao criar requisição Meta: %v", err)
	}
	req.Header.Add("Authorization", "Bearer "+config.AccessToken)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("erro ao enviar requisição Meta: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		bodyBytes, _ := ioutil.ReadAll(res.Body)
		return false, fmt.Errorf("erro Meta status %d: %s", res.StatusCode, string(bodyBytes))
	}

	return true, nil
}
