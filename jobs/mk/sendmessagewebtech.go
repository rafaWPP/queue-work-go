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
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

// -----------------------------------------------------------------------------
// STRUCT PRINCIPAL DO JOB
// -----------------------------------------------------------------------------

// ProcessCampaignJob representa um job para enviar uma única mensagem de uma campanha
// usando API_KEY_3 / API_URL_3.
type ProcessCampaignJob struct {
	CampaignID               int               `json:"campaign_id"`      // ID da campanha
	InstanceName             string            `json:"instance_name"`    // Nome/ID da instância do WhatsApp
	Text                     string            `json:"text"`             // Texto final (tags/spintax já resolvidos) recebido do Laravel
	MediaURL                 string            `json:"media_url"`        // URL da mídia (se houver) recebida do Laravel
	Phone                    string            `json:"phone"`            // Telefone informado no payload
	CampaignSendID           int               `json:"campaign_send_id"` // ID do registro de envio para atualizar status
	SkipDelay                bool              `json:"skip_delay"`       // Se true, não aplica delay local (available_at já agenda)
	MetaTemplate             bool              `json:"meta_template"`
	TemplateName             string            `json:"template_name"`
	TemplateLanguage         string            `json:"template_language"`
	TemplateParameters       []string          `json:"template_parameters"`
	TemplateButtonParameters map[string]string `json:"template_button_parameters"`
	TemplateButtonTypes      map[string]string `json:"template_button_types"`
	HeaderFormat             string            `json:"header_format"`
	HeaderMediaURL           string            `json:"header_media_url"`
	AutoReplyMessage         string            `json:"auto_reply_message"`
	AutoReplyMediaURL        string            `json:"auto_reply_media_url"`
}

type webTechAPIError struct {
	Message           string
	StatusMessage     string
	SendStatus        string
	WhatsAppErrorCode int
	HTTPStatus        int
	RawBody           string
}

func (e *webTechAPIError) Error() string {
	parts := make([]string, 0, 4)

	if message := strings.TrimSpace(e.Message); message != "" {
		parts = append(parts, message)
	}
	if statusMessage := strings.TrimSpace(e.StatusMessage); statusMessage != "" && !strings.EqualFold(statusMessage, e.Message) {
		parts = append(parts, statusMessage)
	}
	if sendStatus := strings.TrimSpace(e.SendStatus); sendStatus != "" {
		parts = append(parts, "send_status="+sendStatus)
	}
	if e.WhatsAppErrorCode > 0 {
		parts = append(parts, fmt.Sprintf("whatsapp_error=%d", e.WhatsAppErrorCode))
	}

	if len(parts) == 0 {
		if strings.TrimSpace(e.RawBody) != "" {
			return e.RawBody
		}
		return "erro desconhecido na API WebTech"
	}

	return strings.Join(parts, " | ")
}

func (e *webTechAPIError) CampaignStatus() string {
	switch {
	case e.WhatsAppErrorCode == 463 || strings.EqualFold(strings.TrimSpace(e.SendStatus), "rejected_by_whatsapp"):
		return "Rejeitado WhatsApp (463)"
	case e.HTTPStatus == 404:
		return "WhatsApp inexistente"
	case strings.Contains(strings.ToLower(e.Error()), "instância desconectada"):
		return "Instância desconectada"
	case strings.TrimSpace(e.StatusMessage) != "":
		return truncateStatusLabel(e.StatusMessage)
	case strings.TrimSpace(e.Message) != "":
		return truncateStatusLabel(e.Message)
	default:
		return "erro"
	}
}

func truncateStatusLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "erro"
	}
	if len(value) <= 120 {
		return value
	}
	return value[:120]
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
	if campStatus == "pause" {
		fmt.Printf("Campanha %d está pausada; ignorando envio.\n", j.CampaignID)
		if j.CampaignSendID > 0 {
			updateCampaignSendStatus(j.CampaignSendID, "pendente")
		}
		return nil
	}

	// Atualiza o status do registro para "enviando"
	if j.CampaignSendID > 0 {
		updateCampaignSendStatus(j.CampaignSendID, "enviando")
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

	// 3) Aplica delay conforme as configurações (se não estiver pré-agendado)
	if !j.SkipDelay {
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
	} else if limite > 0 && messagesSent >= limite {
		resetMessagesSent(j.CampaignID)
		messagesSent = 0
	}

	// 4) Verifica se o telefone foi informado no payload
	if j.Phone == "" {
		fmt.Println("Telefone vazio no payload; abortando envio.")
		if j.CampaignSendID > 0 {
			updateCampaignSendStatus(j.CampaignSendID, "Sem WhatsApp")
		}
		finalizeCampaignIfDone(j.CampaignID)
		return nil
	}

	if j.MetaTemplate && j.TemplateName != "" {
		sent, err := sendMetaTemplate(j, companyID)
		if err != nil {
			if j.CampaignSendID > 0 {
				updateCampaignSendStatus(j.CampaignSendID, "erro")
			}
			fmt.Printf("Erro ao enviar template Meta: %v\n", err)
			finalizeCampaignIfDone(j.CampaignID)
			return nil
		}
		if sent {
			if j.CampaignSendID > 0 {
				updateCampaignSendStatus(j.CampaignSendID, "Enviado")
			}
			incrementMessagesSent(j.CampaignID)
			finalizeCampaignIfDone(j.CampaignID)
			return nil
		}
	}

	// 5) Envia a mensagem via API usando API_KEY_3 / API_URL_3
	errSend := sendMessageAPI3(j.InstanceName, j.Phone, j.Text, j.MediaURL)
	if errSend != nil {
		if j.CampaignSendID > 0 {
			if apiErr, ok := errSend.(*webTechAPIError); ok {
				updateCampaignSendStatus(j.CampaignSendID, apiErr.CampaignStatus())
			} else if strings.Contains(errSend.Error(), "whatsapp não existe") {
				updateCampaignSendStatus(j.CampaignSendID, "WhatsApp inexistente")
			} else {
				updateCampaignSendStatus(j.CampaignSendID, "erro")
			}
		}
		fmt.Printf("Erro ao enviar para %s: %v\n", j.Phone, errSend)
		finalizeCampaignIfDone(j.CampaignID)
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

type MetaConfig struct {
	AccessToken   string
	PhoneNumberID string
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

func sendMetaTemplate(job *ProcessCampaignJob, companyID int) (bool, error) {
	if !job.MetaTemplate || job.TemplateName == "" {
		return false, nil
	}
	config, err := getMetaConfig(companyID)
	if err != nil {
		return false, err
	}
	language := job.TemplateLanguage
	if language == "" {
		language = "pt_BR"
	}

	payload := map[string]interface{}{
		"messaging_product": "whatsapp",
		"to":                job.Phone,
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
		return false, fmt.Errorf("erro ao converter payload Meta: %v", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return false, fmt.Errorf("erro ao criar requisicao Meta: %v", err)
	}
	req.Header.Add("Authorization", "Bearer "+config.AccessToken)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("erro ao enviar requisicao Meta: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		bodyBytes, _ := ioutil.ReadAll(res.Body)
		return false, fmt.Errorf("erro Meta status %d: %s", res.StatusCode, string(bodyBytes))
	}

	// Auto reply (texto + mídia) só é enviado quando o contato RESPONDER (via webhook Meta)
	return true, nil
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

func finalizeCampaignIfDone(campaignID int) {
	restantes, err := getCampaignSendsPendentes(campaignID)
	if err != nil {
		fmt.Printf("Erro ao verificar pendentes da campanha %d: %v\n", campaignID, err)
		return
	}
	if len(restantes) == 0 {
		updateCampaignStatus(campaignID, "enviado")
		resetMessagesSent(campaignID)
		fmt.Printf("Campanha %d finalizada.\n", campaignID)
	}
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
	apiKey := os.Getenv("API_KEY_3")    // Usamos API_KEY_3
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
			"options": map[string]interface{}{
				"presence": "composing",
			},
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
			"options": map[string]interface{}{
				"presence": "composing",
			},
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
		Message           string                 `json:"message"`
		Status            int                    `json:"status"`
		StatusMessage     string                 `json:"statusMessage"`
		Success           bool                   `json:"success"`
		Data              map[string]interface{} `json:"data"`
		SendStatus        string                 `json:"send_status"`
		WhatsAppErrorCode int                    `json:"whatsapp_error_code"`
		Error             string                 `json:"error"`
	}
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return fmt.Errorf("Erro ao decodificar resposta da API: %v", err)
	}

	// Se o envio foi bem-sucedido, retorna nil
	if apiResponse.Success {
		fmt.Println("Mensagem enviada com sucesso:", string(body))
		return nil
	}

	message := strings.TrimSpace(apiResponse.Error)
	if message == "" {
		message = strings.TrimSpace(apiResponse.Message)
	}
	if message == "" {
		message = strings.TrimSpace(apiResponse.StatusMessage)
	}
	if message == "" {
		message = "Erro desconhecido"
	}

	return &webTechAPIError{
		Message:           message,
		StatusMessage:     strings.TrimSpace(apiResponse.StatusMessage),
		SendStatus:        strings.TrimSpace(apiResponse.SendStatus),
		WhatsAppErrorCode: apiResponse.WhatsAppErrorCode,
		HTTPStatus:        apiResponse.Status,
		RawBody:           string(body),
	}
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
