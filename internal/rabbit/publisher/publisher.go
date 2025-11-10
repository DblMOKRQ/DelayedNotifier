package publisher

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"sync"
)

type Publisher struct {
	conn            *amqp.Connection
	ch              *amqp.Channel
	mu              sync.Mutex
	workQueue       string
	delayedExchange string
	workRoutingKey  string
	log             *zap.Logger
}

func NewPublisher(amqpURL string, workQueue string, delayedExchange string, workRoutingKey string, log *zap.Logger) (*Publisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	args := amqp.Table{
		"x-delayed-type": "direct",
	}
	err = ch.ExchangeDeclare(
		delayedExchange,
		"x-delayed-message",
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to declare delayed exchange: %w", err)
	}

	_, err = ch.QueueDeclare(workQueue, true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to declare work queue: %w", err)
	}

	err = ch.QueueBind(workQueue, workRoutingKey, delayedExchange, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to bind work queue to delayed exchange: %w", err)
	}

	return &Publisher{
		conn:            conn,
		ch:              ch,
		workQueue:       workQueue,
		delayedExchange: delayedExchange,
		workRoutingKey:  workRoutingKey,
		log:             log.Named("publisher"),
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, id string, delay int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.Debug("Sending a delayed message", zap.String("id", id), zap.Int64("delay_ms", delay))

	err := p.ch.PublishWithContext(
		ctx,
		p.delayedExchange,
		p.workRoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(id),
			Headers: amqp.Table{
				"x-delay": delay,
			},
		},
	)

	if err != nil {
		p.log.Error("Failed to publish a message", zap.Error(err))
		return fmt.Errorf("error sending a message %w", err)
	}

	return nil
}
func (p *Publisher) Close() error {
	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			p.log.Error("Failed to close channel", zap.Error(err))
			return fmt.Errorf("failed to close channel: %w", err)
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			p.log.Error("Failed to close connection", zap.Error(err))
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}
	return nil
}
