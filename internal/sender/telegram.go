package sender

import (
	"DelayedNotifier/internal/models"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type TelegramSender struct {
	botToken   string
	httpClient *http.Client
}

func NewTelegramSender(botToken string) *TelegramSender {
	return &TelegramSender{
		botToken: botToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}
func (t *TelegramSender) Send(ctx context.Context, notification models.NotificationResponse) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.botToken)

	payload := models.TelegramPayload{
		ChatID: notification.Recipient,
		Text:   notification.Message,
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to decode json for telegram", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to create http request for telegram", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send telegram", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var apiResp struct {
			Description string `json:"description"`
		}
		json.NewDecoder(resp.Body).Decode(&apiResp)
		return fmt.Errorf("ошибка от API Telegram (статус %d): %s", resp.StatusCode, apiResp.Description)
	}
	return nil
}
