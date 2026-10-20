package repository

import (
	"encoding/json"
	"fmt"

	rabbitmqgateway "notification-microservice/infra/rabbitmq-gateway"
)

type NotificationPublisher interface {
	PublishNotification(message []byte) error
}

type NotificationRepository struct {
	publisher NotificationPublisher
}

func NewNotificationRepository(publisher NotificationPublisher) *NotificationRepository {
	return &NotificationRepository{publisher: publisher}
}

func (r *NotificationRepository) Publish(notification any) error {
	message, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("encode notification: %w", err)
	}

	if err := r.publisher.PublishNotification(message); err != nil {
		return fmt.Errorf("publish notification: %w", err)
	}
	return nil
}

type RabbitMQNotificationPublisher struct{}

func (RabbitMQNotificationPublisher) PublishNotification(message []byte) error {
	return rabbitmqgateway.PublishNotification(message)
}
