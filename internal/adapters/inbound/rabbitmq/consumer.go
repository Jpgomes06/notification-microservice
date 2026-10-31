package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"

	"notification-microservice/domain"
	"notification-microservice/internal/ports/inbound"
)

type Consumer struct {
	processor  inbound.NotificationProcessor
	deliveries <-chan amqp.Delivery
}

func NewConsumer(processor inbound.NotificationProcessor, deliveries <-chan amqp.Delivery) *Consumer {
	return &Consumer{processor: processor, deliveries: deliveries}
}

func (c *Consumer) ProcessMessage(ctx context.Context, msg amqp.Delivery) error {
	var notification domain.Notification
	if err := json.Unmarshal(msg.Body, &notification); err != nil {
		return fmt.Errorf("decode notification payload: %w", err)
	}

	if notification.CreatedAt == "" {
		notification.CreatedAt = time.Now().Format(time.RFC3339)
	}

	return c.processor.Process(ctx, notification)
}

func (c *Consumer) Start(ctx context.Context) error {
	logrus.Info("Starting notification consumer, listening for messages...")
	for msg := range c.deliveries {
		c.handleDelivery(ctx, msg)
	}
	logrus.Warn("RabbitMQ delivery channel closed, stopping consumer")
	return nil
}

func (c *Consumer) handleDelivery(ctx context.Context, msg amqp.Delivery) {
	if ctx.Err() != nil {
		logrus.Info("Notification consumer stopped due to context cancellation")
		return
	}

	if err := c.ProcessMessage(ctx, msg); err != nil {
		logrus.Errorf("Error processing notification message: %v", err)
		logDeliveryError("reject", msg.Nack(false, false))
		return
	}

	logDeliveryError("acknowledge", msg.Ack(false))
}

func logDeliveryError(action string, err error) {
	if err != nil {
		logrus.Errorf("Failed to %s RabbitMQ message: %v", action, err)
	}
}
