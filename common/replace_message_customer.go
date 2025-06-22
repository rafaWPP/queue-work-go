package common

import (
	"strings"
	"time"
)

// ReplaceMessageCustomer substitui as tags na mensagem pelo conteúdo do cliente.
func ReplaceMessageCustomer(messageText string, customer map[string]string) string {
	// Substitui as tags pelos valores correspondentes do cliente
	replacements := map[string]string{
		"{{customer_id}}":           customer["id"],
		"{{customer_name}}":         customer["name"],
		"{{customer_first_name}}":   getFirstName(customer["name"]),
		"{{customer_whatsapp}}":     customer["whatsapp"],
		"{{customer_email}}":        customer["email"],
		"{{customer_usuario}}":      customer["usuario"],
		"{{customer_password}}":     customer["password"],
		"{{company_name}}":          customer["company_name"],
		"{{user_attendant}}":        customer["user_attendant"],
		"{{customer_product_name}}": customer["product_name"],
		"{{customer_plan_name}}":    customer["plan_name"],
		"{{customer_plan_value}}":   customer["plan_value"],
		"{{customer_profile_url}}":  customer["profile_url"],
		"{{customer_duedate}}":      formatDate(customer["data_de_vencimento"]),
		"{{customer_created_at}}":   formatDate(customer["created_at"]),
		"{{customer_updated_at}}":   formatDate(customer["updated_at"]),
		"{{customer_last_notification_date}}": formatDate(customer["last_notification_date"]),
		"{{day_sh}}":  time.Now().Format("02/01/2006"),
		"{{day}}":     time.Now().Format("02/01/2006 15:04"),
	}

	// Aplicar as substituições na mensagem
	for tag, value := range replacements {
		messageText = strings.ReplaceAll(messageText, tag, value)
	}

	return messageText
}

// getFirstName extrai o primeiro nome do cliente
func getFirstName(fullName string) string {
	names := strings.Split(fullName, " ")
	if len(names) > 0 {
		return names[0]
	}
	return fullName
}

// formatDate formata a data para "dd/mm/yyyy"
func formatDate(dateStr string) string {
	if dateStr == "" {
		return ""
	}
	parsedDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return ""
	}
	return parsedDate.Format("02/01/2006")
}
