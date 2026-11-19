package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"go-queue/common"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type TelegramPayload struct {
	BotToken      string `json:"bot_token"`
	ChatID        string `json:"chat_id"`
	Text          string `json:"text"`
	Media         string `json:"media"`
	Client        string `json:"client"`
	SendMessageID int    `json:"send_message_id"`
}

type TelegramJob struct {
	payload TelegramPayload
}

func New(data []byte) common.Job {
	var payload TelegramPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil
	}
	return &TelegramJob{payload: payload}
}

func (j *TelegramJob) Handle() error {
	if j.payload.BotToken == "" || j.payload.ChatID == "" || j.payload.SendMessageID <= 0 {
		return fmt.Errorf("payload telegram inválido")
	}

	if err := updateTelegramStatus(j.payload.Client, j.payload.SendMessageID, "sending"); err != nil {
		return err
	}

	if err := sendTelegramPayload(j.payload); err != nil {
		_ = updateTelegramStatus(j.payload.Client, j.payload.SendMessageID, "erro")
		return err
	}

	return updateTelegramStatus(j.payload.Client, j.payload.SendMessageID, "sent")
}

func sendTelegramPayload(payload TelegramPayload) error {
	message := strings.TrimSpace(payload.Text)
	if payload.Media != "" {
		method := resolveTelegramMediaMethod(payload.Media)
		field := "document"
		if method == "sendPhoto" {
			field = "photo"
		}

		caption := message
		sendExtraText := false
		if len(caption) > 1000 {
			caption = caption[:1000]
			sendExtraText = true
		}

		values := url.Values{}
		values.Set("chat_id", payload.ChatID)
		values.Set(field, payload.Media)
		if caption != "" {
			values.Set("caption", caption)
		}

		if err := telegramRequest(payload.BotToken, method, values); err != nil {
			return err
		}

		if sendExtraText {
			return telegramRequest(payload.BotToken, "sendMessage", url.Values{
				"chat_id": {payload.ChatID},
				"text":    {message},
			})
		}

		return nil
	}

	if message == "" {
		message = " "
	}

	return telegramRequest(payload.BotToken, "sendMessage", url.Values{
		"chat_id": {payload.ChatID},
		"text":    {message},
	})
}

func resolveTelegramMediaMethod(mediaURL string) string {
	parsedURL, err := url.Parse(mediaURL)
	if err != nil {
		return "sendDocument"
	}

	ext := strings.ToLower(filepath.Ext(parsedURL.Path))
	if ext == "" {
		return "sendDocument"
	}

	mimeType := mime.TypeByExtension(ext)
	if strings.HasPrefix(mimeType, "image/") {
		return "sendPhoto"
	}

	return "sendDocument"
}

func telegramRequest(botToken, method string, values url.Values) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("https://api.telegram.org/bot%s/%s", botToken, method), bytes.NewBufferString(values.Encode()))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var response struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}

	if !response.OK {
		if response.Description == "" {
			response.Description = "erro desconhecido ao enviar Telegram"
		}
		return fmt.Errorf(response.Description)
	}

	return nil
}

func updateTelegramStatus(clientType string, recordID int, status string) error {
	dsn, err := getTelegramDSN()
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	table := "send_messages_all"
	if clientType == "user" {
		table = "sendmessageuser"
	}

	_, err = db.Exec(fmt.Sprintf("UPDATE %s SET status = ?, updated_at = NOW() WHERE id = ?", table), status, recordID)
	return err
}

func getTelegramDSN() (string, error) {
	if err := godotenv.Load(); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	), nil
}
