package models

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrNotFound = fmt.Errorf("notifier not found")
)

type NotificationStatus string

const (
	StatusPending   NotificationStatus = "PENDING"
	StatusSent      NotificationStatus = "SENT"
	StatusFailed    NotificationStatus = "FAILED"
	StatusCancelled NotificationStatus = "CANCELLED"
	StatusRetry     NotificationStatus = "RETRY"
)

type Notification struct {
	ID            string    `json:"id,omitempty"`
	Channel       string    `json:"channel"`
	SendAt        time.Time `json:"send_at"`
	Recipient     string    `json:"recipient"`
	Message       string    `json:"message"`
	RetryAttempts int       `json:"retry_attempts omitempty"`
	NextSendAt    time.Time `json:"next_send_at omitempty"`
}

func (n *Notification) Validate() error {
	if strings.TrimSpace(n.Channel) == "" {
		return errors.New("'channel' field cannot be empty")
	}
	if strings.TrimSpace(n.Recipient) == "" {
		return errors.New("'recipient' field cannot be empty")
	}
	if strings.TrimSpace(n.Message) == "" {
		return errors.New(" 'message' field cannot be empty")
	}
	if n.SendAt.Before(time.Now().Add(-1 * time.Minute)) {
		return errors.New("'send_at' field cannot be in the past")
	}
	return nil
}

type NotificationResponse struct {
	ID            string             `json:"id"`
	Channel       string             `json:"channel"`
	Status        NotificationStatus `json:"status"`
	SendAt        time.Time          `json:"send_at"`
	Recipient     string             `json:"recipient"`
	Message       string             `json:"message"`
	RetryAttempts int                `json:"retry_attempts"`
	NextSendAt    time.Time          `json:"next_send_at"`
}

type TelegramPayload struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}
