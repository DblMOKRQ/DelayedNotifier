package consumer

import (
	"DelayedNotifier/internal/service"
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

type Consumer struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue amqp.Queue
	serv  *service.Service
	log   *zap.Logger
}

func NewConsumer(amqpURL string, serv *service.Service, workQueue string, log *zap.Logger) (*Consumer, error) {
	conn, err := amqp.Dial(amqpURL)

	if err != nil {
		log.Error("Failed to connect to RabbitMQ", zap.Error(err))
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		log.Error("Failed to open a channel", zap.Error(err))
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	queue, err := ch.QueueDeclare(
		workQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = conn.Close()
		_ = ch.Close()
		log.Error("Failed to declare a queue", zap.Error(err))
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		_ = conn.Close()
		_ = ch.Close()
		log.Error("Failed to set QoS", zap.Error(err))
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &Consumer{
		conn:  conn,
		ch:    ch,
		queue: queue,
		serv:  serv,
		log:   log.Named("consumer"),
	}, nil
}
func (c *Consumer) StartListening(ctx context.Context, maxAttempts int) {
	if c.ch == nil {
		c.log.Error("no channel available")
		return
	}

	msgs, err := c.ch.Consume(
		c.queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		c.log.Error("failed to register a consumer", zap.Error(err))
		return
	}

	for {
		select {
		case <-ctx.Done():
			c.log.Info("Context cancelled, stopping consumer...")
			return
		case msg, ok := <-msgs:
			if !ok {
				c.log.Info("Message channel closed, consumer stopping.")
				return
			}
			c.log.Info(fmt.Sprintf("Received a message: %s", msg.Body))
			processCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			if err := c.serv.ProcessNotificationFromQueue(processCtx, string(msg.Body), maxAttempts); err != nil {
				c.log.Warn("failed to process a message, nacking", zap.Error(err))
				msg.Nack(false, true)
			} else {
				msg.Ack(false)
				c.log.Info("Message processed successfully, acknowledged", zap.String("ID", string(msg.Body)))
			}
			cancel()
		}
	}
}

func (c *Consumer) Close() error {
	c.log.Info("Closing consumer connections...")
	if c.ch != nil {
		if err := c.ch.Close(); err != nil {
			c.log.Error("Failed to close channel", zap.Error(err))
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			c.log.Error("Failed to close connection", zap.Error(err))
		}
	}
	return nil
}
