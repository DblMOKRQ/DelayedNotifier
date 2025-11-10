package service

import (
	"DelayedNotifier/internal/models"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"math"
	"time"
)

type Publisher interface {
	Publish(ctx context.Context, id string, ttl int64) error
	Close() error
}

type Repo interface {
	CreateNotifier(ctx context.Context, notification *models.Notification) error
	GetNotifier(ctx context.Context, id string) (*models.NotificationResponse, error)
	SetStatus(ctx context.Context, id string, status models.NotificationStatus) error
	UpdateForRetry(ctx context.Context, countRetry int, nextSendAt time.Time, status models.NotificationStatus, id string) error
}

type Sender interface {
	Send(ctx context.Context, notification models.NotificationResponse) error
}

type Service struct {
	publisher Publisher
	repo      Repo
	sender    Sender
	log       *zap.Logger
}

func NewService(publisher Publisher, repo Repo, send Sender, log *zap.Logger) *Service {
	return &Service{
		publisher: publisher,
		repo:      repo,
		sender:    send,
		log:       log.Named("service"),
	}
}

// Create создает уведомление, заносит его в бд и в очередь
func (s *Service) Create(ctx context.Context, notification models.Notification) (string, error) {
	id := uuid.New().String()
	notification.ID = id
	notification.NextSendAt = notification.SendAt
	notification.RetryAttempts = 0
	s.log.Debug("Creating Notifier", zap.String("id", notification.ID))
	if err := s.repo.CreateNotifier(ctx, &notification); err != nil {
		s.log.Error("Failed to create notifier", zap.String("id", notification.ID), zap.Error(err))
		return "", fmt.Errorf("failed to create notifier: %w", err)
	}

	delay := notification.SendAt.UnixMilli() - time.Now().UTC().UnixMilli()
	if delay < 0 {
		delay = 0
	}

	if err := s.publisher.Publish(ctx, id, delay); err != nil {
		s.log.Error("Failed to publish notifier", zap.String("id", notification.ID), zap.Error(err))
		return "", fmt.Errorf("failed to publish message: %w", err)
	}
	s.log.Debug("Notifier created", zap.String("id", notification.ID))
	return id, nil
}

// Get получение уведомления из бд
func (s *Service) Get(ctx context.Context, id string) (*models.NotificationResponse, error) {
	s.log.Debug("Getting Notifier", zap.String("id", id))
	notifier, err := s.repo.GetNotifier(ctx, id)
	if err != nil {
		if errors.Is(err, models.ErrNotFound) {
			s.log.Warn("Notifier not found", zap.String("id", id))
			return nil, models.ErrNotFound
		}
		s.log.Error("Failed to get notifier", zap.String("id", id), zap.Error(err))
		return nil, fmt.Errorf("failed to get notifier: %w", err)
	}
	s.log.Debug("Notifier retrieved", zap.String("id", id))
	return notifier, nil
}

// Delete удаление уведомления из бд
func (s *Service) Delete(ctx context.Context, id string) error {
	s.log.Debug("Deleting Notifier", zap.String("id", id))
	if err := s.repo.SetStatus(ctx, id, models.StatusCancelled); err != nil {
		if errors.Is(err, models.ErrNotFound) {
			s.log.Warn("Notifier not found", zap.String("id", id))
			return err
		}
		s.log.Error("Failed to set status", zap.String("id", id), zap.Error(err))
		return fmt.Errorf("failed to delete notifier: %w", err)
	}
	s.log.Debug("Notifier deleted", zap.String("id", id))
	return nil
}

func (s *Service) ProcessNotificationFromQueue(ctx context.Context, id string, maxAttempts int) error {
	s.log.Debug("Processing Notifier", zap.String("id", id))
	notifier, err := s.repo.GetNotifier(ctx, id)
	if err != nil {
		s.log.Error("Failed to get notifier", zap.String("id", id), zap.Error(err))
		return fmt.Errorf("failed to get notifier: %w", err)
	}
	if notifier.Status == models.StatusCancelled {
		s.log.Debug("Notifier is cancelled", zap.String("id", id))
		return nil
	}
	if notifier.Status != models.StatusPending && notifier.Status != models.StatusRetry {
		s.log.Warn("Notifier is not pending and not retry", zap.String("id", id))
		return nil
	}

	if err := s.sender.Send(ctx, *notifier); err != nil {
		if notifier.RetryAttempts == maxAttempts {
			s.log.Error("Failed to send notifier after max attempts", zap.String("id", id))
			if err := s.repo.SetStatus(ctx, id, models.StatusFailed); err != nil {
				s.log.Error("Failed to set status after max attempts", zap.String("id", id), zap.Error(err))
				return fmt.Errorf("failed to set status after max attempts: %w", err)
			}
			return nil
		}
		notifier.RetryAttempts = notifier.RetryAttempts + 1

		delaySeconds := math.Pow(2, float64(notifier.RetryAttempts))
		delay := time.Duration(delaySeconds) * time.Second

		nextSendAt := time.Now().Add(delay)
		if err := s.repo.UpdateForRetry(ctx, notifier.RetryAttempts, nextSendAt, models.StatusRetry, notifier.ID); err != nil {
			s.log.Error("Failed to add attempts", zap.String("id", id), zap.Error(err))
			return fmt.Errorf("failed to add attempts: %w", err)
		}

		s.repo.SetStatus(ctx, id, models.StatusRetry)
		delayMilliseconds := delay.Milliseconds()
		if err := s.publisher.Publish(ctx, notifier.ID, delayMilliseconds); err != nil {
			s.log.Error("Failed to publish notifier", zap.String("id", id), zap.Error(err))
			return fmt.Errorf("failed to publish message: %w", err)
		}
		s.log.Warn("Failed to send notifier, re-publishing for retry",
			zap.String("id", id),
			zap.Int("new_attempt", notifier.RetryAttempts),
			zap.Duration("delay", delay),
			zap.Error(err),
		)
		return nil
	}
	if err := s.repo.SetStatus(ctx, id, models.StatusSent); err != nil {
		s.log.Error("Failed to set status", zap.String("id", id), zap.Error(err))
		s.repo.SetStatus(ctx, id, models.StatusFailed)
		return fmt.Errorf("failed to set notification status: %w", err)
	}
	s.log.Debug("Notifier sent", zap.String("id", id))
	return nil
}

func (s *Service) ClosePublisher() error {
	return s.publisher.Close()
}
