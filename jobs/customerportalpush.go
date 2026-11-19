package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"go-queue/common"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/SherClockHolmes/webpush-go"
)

type CustomerPortalPushJob struct {
	NotificationKind  string                 `json:"notification_kind"`
	CustomerID        int                    `json:"customer_id"`
	Title             string                 `json:"title"`
	Body              string                 `json:"body"`
	SendMessageID     *int                   `json:"send_message_id"`
	CustomerPaymentID *int                   `json:"customer_payment_id"`
	Data              map[string]interface{} `json:"data"`
}

type customerAppNotificationRecord struct {
	ID    int64
	Title string
	Body  string
	Data  map[string]interface{}
}

type customerPushSubscriptionRecord struct {
	ID              int64
	Endpoint        string
	PublicKey       string
	AuthToken       string
	ContentEncoding string
}

func New(data []byte) common.Job {
	var job CustomerPortalPushJob
	if err := json.Unmarshal(data, &job); err != nil {
		return &customerPortalPushInvalidJob{Err: fmt.Errorf("payload inválido do customerportalpush: %w", err)}
	}

	return &job
}

type customerPortalPushInvalidJob struct {
	Err error
}

func (j *customerPortalPushInvalidJob) Handle() error {
	return j.Err
}

func (j *CustomerPortalPushJob) Handle() error {
	if j.CustomerID <= 0 {
		return fmt.Errorf("customer_id inválido")
	}

	if j.Data == nil {
		j.Data = make(map[string]interface{})
	}

	dsn, err := getCustomerPortalPushDSN()
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("erro ao conectar ao banco: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("erro ao validar conexão com banco: %w", err)
	}

	existing, err := findExistingNotification(db, j)
	if err != nil {
		return err
	}

	if existing != nil {
		return nil
	}

	record, err := insertNotification(db, j)
	if err != nil {
		return err
	}

	unreadCount, err := unreadCount(db, j.CustomerID)
	if err != nil {
		return err
	}

	pushPayload, err := buildPushPayload(record, unreadCount)
	if err != nil {
		return err
	}

	sent, err := sendPushToActiveSubscriptions(db, j.CustomerID, pushPayload)
	if err != nil {
		return err
	}

	if sent {
		if err := markNotificationPushed(db, record.ID); err != nil {
			return err
		}
	}

	return nil
}

func getCustomerPortalPushDSN() (string, error) {
	laravelRoot := resolveLaravelRoot()
	laravelEnv := laravelRoot + "/.env"

	_ = godotenv.Load(laravelEnv)
	_ = godotenv.Load()

	dbUser := strings.TrimSpace(os.Getenv("DB_USER"))
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := strings.TrimSpace(os.Getenv("DB_NAME"))
	dbHost := strings.TrimSpace(os.Getenv("DB_HOST"))
	dbPort := strings.TrimSpace(os.Getenv("DB_PORT"))

	if dbUser == "" || dbName == "" || dbHost == "" || dbPort == "" {
		return "", fmt.Errorf("variáveis de banco não configuradas para customerportalpush")
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName), nil
}

func resolveLaravelRoot() string {
	if root := strings.TrimSpace(os.Getenv("NPANEL_ROOT")); root != "" {
		return root
	}

	return "/home/newgestor-npanel/htdocs/npanel.newgestor.com"
}

func findExistingNotification(db *sql.DB, job *CustomerPortalPushJob) (*customerAppNotificationRecord, error) {
	if job.NotificationKind == "message" && job.SendMessageID != nil {
		row := db.QueryRow(`
			SELECT id, title, body, data
			FROM customer_app_notifications
			WHERE customer_id = ?
			  AND type = 'message'
			  AND JSON_UNQUOTE(JSON_EXTRACT(data, '$.send_message_id')) = ?
			  AND JSON_UNQUOTE(JSON_EXTRACT(data, '$.channel')) = 'app'
			ORDER BY id DESC
			LIMIT 1
		`, job.CustomerID, fmt.Sprintf("%d", *job.SendMessageID))

		return scanNotificationRow(row)
	}

	if job.NotificationKind == "payment_confirmation" && job.CustomerPaymentID != nil {
		row := db.QueryRow(`
			SELECT id, title, body, data
			FROM customer_app_notifications
			WHERE customer_id = ?
			  AND type = 'payment_confirmation'
			  AND JSON_UNQUOTE(JSON_EXTRACT(data, '$.customer_payment_id')) = ?
			  AND JSON_UNQUOTE(JSON_EXTRACT(data, '$.channel')) = 'app'
			ORDER BY id DESC
			LIMIT 1
		`, job.CustomerID, fmt.Sprintf("%d", *job.CustomerPaymentID))

		return scanNotificationRow(row)
	}

	return nil, nil
}

func scanNotificationRow(row *sql.Row) (*customerAppNotificationRecord, error) {
	var record customerAppNotificationRecord
	var dataRaw sql.NullString

	err := row.Scan(&record.ID, &record.Title, &record.Body, &dataRaw)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, fmt.Errorf("erro ao consultar notificação existente: %w", err)
	}

	record.Data = parseJSONMap(dataRaw.String)

	return &record, nil
}

func insertNotification(db *sql.DB, job *CustomerPortalPushJob) (*customerAppNotificationRecord, error) {
	title, bodyPreview, notifType, data := normalizeNotification(job)
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("erro ao serializar data da notificação: %w", err)
	}

	result, err := db.Exec(`
			INSERT INTO customer_app_notifications
				(customer_id, type, title, body, data, sent_at, created_at, updated_at)
			VALUES
				(?, ?, ?, ?, ?, NOW(), NOW(), NOW())
		`,
		job.CustomerID,
		notifType,
		title,
		bodyPreview,
		string(dataJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("erro ao inserir customer_app_notifications: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter id da notificação inserida: %w", err)
	}

	return &customerAppNotificationRecord{
		ID:    id,
		Title: title,
		Body:  bodyPreview,
		Data:  data,
	}, nil
}

func normalizeNotification(job *CustomerPortalPushJob) (string, string, string, map[string]interface{}) {
	fullBody := normalizeText(job.Body)
	bodyPreview := limitText(fullBody, 180)
	data := cloneMap(job.Data)

	notifType := "message"
	title := stringFromMap(data, "company_name")

	switch job.NotificationKind {
	case "payment_confirmation":
		notifType = "payment_confirmation"
		if title == "" {
			title = strings.TrimSpace(job.Title)
		}
		data["type"] = "payment_confirmation"
		data["full_body"] = fullBody
		if job.CustomerPaymentID != nil {
			data["customer_payment_id"] = *job.CustomerPaymentID
		}
	case "generic":
		if title == "" {
			title = strings.TrimSpace(job.Title)
		}
		if title == "" {
			title = "Notificação"
		}
		if typeValue := strings.TrimSpace(stringFromMap(data, "type")); typeValue != "" {
			notifType = typeValue
		}
		data["full_body"] = fullBody
	default:
		if title == "" {
			title = "Notificação"
		}
		data["type"] = "message"
		data["full_body"] = fullBody
		if job.SendMessageID != nil {
			data["send_message_id"] = *job.SendMessageID
		}
	}

	if _, ok := data["channel"]; !ok {
		data["channel"] = "app"
	}

	return title, bodyPreview, notifType, data
}

func unreadCount(db *sql.DB, customerID int) (int, error) {
	var total int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM customer_app_notifications
		WHERE customer_id = ?
		  AND type != 'portal_update'
		  AND read_at IS NULL
	`, customerID).Scan(&total)

	if err != nil {
		return 0, fmt.Errorf("erro ao contar notificações não lidas: %w", err)
	}

	return total, nil
}

func buildPushPayload(record *customerAppNotificationRecord, unreadCount int) ([]byte, error) {
	data := cloneMap(record.Data)
	basePortalURL := strings.TrimSpace(stringFromMap(data, "base_portal_url"))
	url := strings.TrimSpace(stringFromMap(data, "url"))

	if url == "" && basePortalURL != "" {
		url = fmt.Sprintf("%s?panel=avisos&notification=%d", basePortalURL, record.ID)
	}

	icon := strings.TrimSpace(stringFromMap(data, "company_logo_url"))
	if icon == "" {
		icon = strings.TrimSpace(stringFromMap(data, "fallback_icon_url"))
	}

	title := strings.TrimSpace(stringFromMap(data, "push_title"))
	if title == "" {
		title = strings.TrimSpace(stringFromMap(data, "company_name"))
	}
	if title == "" {
		title = record.Title
	}

	data["notification_id"] = record.ID
	data["url"] = url
	data["unread_count"] = unreadCount

	payload := map[string]interface{}{
		"title":        title,
		"body":         record.Body,
		"url":          url,
		"icon":         icon,
		"badge":        icon,
		"image":        nullableStringFromMap(data, "image_url"),
		"unread_count": unreadCount,
		"data":         data,
	}

	return json.Marshal(payload)
}

func sendPushToActiveSubscriptions(db *sql.DB, customerID int, payload []byte) (bool, error) {
	publicKey, privateKey, subject, err := loadPwaConfig()
	if err != nil {
		return false, err
	}

	subscriptions, err := activeSubscriptions(db, customerID)
	if err != nil {
		return false, err
	}

	if len(subscriptions) == 0 {
		return false, nil
	}

	options := &webpush.Options{
		Subscriber:      subject,
		VAPIDPublicKey:  publicKey,
		VAPIDPrivateKey: privateKey,
		TTL:             60,
	}

	success := false

	for _, sub := range subscriptions {
		resp, err := webpush.SendNotification(payload, &webpush.Subscription{
			Endpoint: sub.Endpoint,
			Keys: webpush.Keys{
				P256dh: sub.PublicKey,
				Auth:   sub.AuthToken,
			},
		}, options)
		if err != nil {
			continue
		}

		if resp == nil {
			continue
		}

		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			success = true
			continue
		}

		if resp.StatusCode == 404 || resp.StatusCode == 410 {
			_, _ = db.Exec(`
				UPDATE customer_push_subscriptions
				SET notification_permission = 'expired',
				    unsubscribed_at = NOW(),
				    updated_at = NOW()
				WHERE id = ?
			`, sub.ID)
		}
	}

	return success, nil
}

func loadPwaConfig() (string, string, string, error) {
	laravelRoot := resolveLaravelRoot()
	laravelEnv := laravelRoot + "/.env"

	_ = godotenv.Load(laravelEnv)
	_ = godotenv.Load()

	publicKey := strings.TrimSpace(os.Getenv("PWA_VAPID_PUBLIC_KEY"))
	privateKey := strings.TrimSpace(os.Getenv("PWA_VAPID_PRIVATE_KEY"))
	subject := strings.TrimSpace(os.Getenv("PWA_VAPID_SUBJECT"))

	if subject == "" {
		subject = "mailto:suporte@newgestor.com"
	}

	if publicKey == "" || privateKey == "" {
		return "", "", "", fmt.Errorf("VAPID do portal não configurado")
	}

	return publicKey, privateKey, subject, nil
}

func activeSubscriptions(db *sql.DB, customerID int) ([]customerPushSubscriptionRecord, error) {
	rows, err := db.Query(`
		SELECT id, endpoint, public_key, auth_token, COALESCE(content_encoding, 'aesgcm')
		FROM customer_push_subscriptions
		WHERE customer_id = ?
		  AND unsubscribed_at IS NULL
	`, customerID)
	if err != nil {
		return nil, fmt.Errorf("erro ao buscar subscriptions ativas: %w", err)
	}
	defer rows.Close()

	items := make([]customerPushSubscriptionRecord, 0)

	for rows.Next() {
		var item customerPushSubscriptionRecord
		if err := rows.Scan(&item.ID, &item.Endpoint, &item.PublicKey, &item.AuthToken, &item.ContentEncoding); err != nil {
			return nil, fmt.Errorf("erro ao ler subscription ativa: %w", err)
		}
		items = append(items, item)
	}

	return items, nil
}

func markNotificationPushed(db *sql.DB, notificationID int64) error {
	_, err := db.Exec(`
		UPDATE customer_app_notifications
		SET pushed_at = NOW(),
		    updated_at = NOW()
		WHERE id = ?
	`, notificationID)

	if err != nil {
		return fmt.Errorf("erro ao atualizar pushed_at da notificação: %w", err)
	}

	return nil
}

func normalizeText(value string) string {
	text := strings.TrimSpace(value)
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")

	lines := strings.Split(text, "\n")
	cleaned := make([]string, 0, len(lines))
	emptyCount := 0

	for _, line := range lines {
		line = strings.Join(strings.Fields(line), " ")
		if line == "" {
			emptyCount++
			if emptyCount > 2 {
				continue
			}
		} else {
			emptyCount = 0
		}
		cleaned = append(cleaned, line)
	}

	return strings.TrimSpace(strings.Join(cleaned, "\n"))
}

func limitText(value string, max int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= max {
		return strings.Join(strings.Fields(strings.ReplaceAll(value, "\n", " ")), " ")
	}

	return strings.TrimSpace(string(runes[:max])) + "..."
}

func parseJSONMap(raw string) map[string]interface{} {
	if strings.TrimSpace(raw) == "" {
		return map[string]interface{}{}
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return map[string]interface{}{}
	}

	return data
}

func cloneMap(source map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func stringFromMap(data map[string]interface{}, key string) string {
	if data == nil {
		return ""
	}

	value, ok := data[key]
	if !ok || value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func nullableStringFromMap(data map[string]interface{}, key string) interface{} {
	value := strings.TrimSpace(stringFromMap(data, key))
	if value == "" {
		return nil
	}

	return value
}

func init() {
	time.Local = time.Now().Location()
}
