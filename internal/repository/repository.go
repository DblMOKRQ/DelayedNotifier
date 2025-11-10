package repository

import (
	"DelayedNotifier/internal/models"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/lib/pq"
	"github.com/wb-go/wbf/dbpg"
	"github.com/wb-go/wbf/retry"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"time"
)

type Repository struct {
	db  *dbpg.DB
	log *zap.Logger
}

var (
	retryStrategy = retry.Strategy{
		Attempts: 5,
		Delay:    time.Millisecond,
		Backoff:  2,
	}
)

const (
	createQuery         = `INSERT INTO  notifications (id, status, send_at,channel, recipient, message,retry_attempts,next_send_at) VALUES ($1, $2, $3, $4, $5,$6,$7,$8)`
	getQuery            = `SELECT * FROM  notifications WHERE id = $1`
	updateQuery         = `UPDATE notifications SET status = $1 WHERE id = $2`
	updateForRetryQuery = `UPDATE notifications SET retry_attempts = $1, next_send_at = $2, status = $3 WHERE id = $4`
)

func NewRepository(masterDSN string, slaveDSNs []string, log *zap.Logger) (*Repository, error) {
	opts := dbpg.Options{
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	}
	db, err := dbpg.New(masterDSN, slaveDSNs, &opts)
	if err != nil {
		log.Fatal("Failed to connect to database", zap.Error(err))
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Info("Starting database migrations")

	if err := runMigrations(masterDSN); err != nil {
		log.Error("Failed to run migrations", zap.Error(err))
		return nil, fmt.Errorf("failed to run migration: %w", err)
	}
	log.Info("Successfully migrated database")

	return &Repository{db: db, log: log.Named("repository")}, nil
}

func (r *Repository) CreateNotifier(ctx context.Context, notification *models.Notification) error {
	r.log.Debug("Creating notifier", zap.Any("notification", notification))
	_, err := r.db.ExecWithRetry(ctx, retryStrategy, createQuery, notification.ID, models.StatusPending, notification.SendAt, notification.Channel, notification.Recipient, notification.Message, notification.RetryAttempts, notification.NextSendAt)
	if err != nil {
		r.log.Error("Failed to create notifier", zap.Error(err))
		return fmt.Errorf("failed to create notifier: %w", err)
	}
	r.log.Debug("Notifier created", zap.Any("notification", notification))
	return nil
}
func (r *Repository) GetNotifier(ctx context.Context, id string) (*models.NotificationResponse, error) {
	r.log.Debug("Getting notifier", zap.String("id", id))
	var notification models.NotificationResponse
	row, err := r.db.QueryRowWithRetry(ctx, retryStrategy, getQuery, id)
	if err != nil {
		r.log.Error("Failed to get notifier", zap.String("id", id), zap.Error(err))
		return nil, fmt.Errorf("failed to get notifier: %w", err)
	}
	if err = row.Scan(&notification.ID, &notification.Channel, &notification.Status, &notification.SendAt, &notification.Recipient, &notification.Message, &notification.RetryAttempts, &notification.NextSendAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			r.log.Debug("Notifier not found", zap.String("id", id))
			return nil, models.ErrNotFound
		}
		r.log.Error("Failed to get notifier", zap.String("id", id), zap.Error(err))
		return nil, fmt.Errorf("failed to get notifier: %w", err)
	}
	r.log.Debug("Notifier found", zap.String("id", id))
	return &notification, nil
}

func (r *Repository) SetStatus(ctx context.Context, id string, status models.NotificationStatus) error {

	r.log.Debug("Updating status", zap.String("id", id), zap.Any("status", status))
	_, err := r.db.ExecWithRetry(ctx, retryStrategy, updateQuery, status, id)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			if pqErr.Code == "42703" {
				r.log.Warn("Notifier not found", zap.String("id", id))
				return models.ErrNotFound
			}
		}
		r.log.Error("Failed to update status", zap.String("id", id), zap.Error(err))
		return fmt.Errorf("failed to update status: %w", err)
	}
	r.log.Debug("Notifier updated", zap.String("id", id))
	return nil
}

func (r *Repository) UpdateForRetry(ctx context.Context, countRetry int, nextSendAt time.Time, status models.NotificationStatus, id string) error {
	r.log.Debug("Update attempts and next send", zap.String("id", id))
	_, err := r.db.ExecWithRetry(ctx, retryStrategy, updateForRetryQuery, countRetry, nextSendAt, status, id)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			if pqErr.Code == "42703" {
				r.log.Warn("Notifier not found", zap.String("id", id))
				return models.ErrNotFound
			}
		}
		r.log.Error("Failed to add attempts", zap.String("id", id), zap.Error(err))
		return fmt.Errorf("failed to add attempts: %w", err)
	}
	r.log.Debug("Successfully update attempts and next send", zap.String("id", id))
	return nil
}

func runMigrations(connStr string) error {
	migratePath := os.Getenv("MIGRATE_PATH")
	if migratePath == "" {
		migratePath = "./migrations"
	}
	absPath, err := filepath.Abs(migratePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}
	absPath = filepath.ToSlash(absPath)
	migrateUrl := fmt.Sprintf("file://%s", absPath)
	m, err := migrate.New(migrateUrl, connStr)
	if err != nil {
		return fmt.Errorf("start migrations error %v", err)
	}
	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			return nil
		}
		return fmt.Errorf("migration up error: %v", err)
	}
	return nil
}
