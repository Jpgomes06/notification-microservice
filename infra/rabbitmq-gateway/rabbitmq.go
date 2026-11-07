package rabbitmqgateway

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	conn       *amqp.Connection
	ch         *amqp.Channel
	consumerCh *amqp.Channel
)

func InitRabbitMQ(url string) error {
	var err error
	conn, err = amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	ch, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	_, err = ch.QueueDeclare(
		"notifications",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}
	return nil
}

func Close() {
	CloseConsumer()
	if ch != nil {
		ch.Close()
	}
	if conn != nil {
		conn.Close()
	}
}

func CloseConsumer() {
	if consumerCh != nil {
		consumerCh.Close()
	}
}

func ConsumeNotifications() (<-chan amqp.Delivery, error) {
	if conn == nil {
		return nil, fmt.Errorf("rabbitmq connection is not initialized")
	}

	var err error
	consumerCh, err = conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open consumer channel: %w", err)
	}

	if err := consumerCh.Qos(10, 0, false); err != nil {
		return nil, fmt.Errorf("failed to set consumer QoS: %w", err)
	}

	msgs, err := consumerCh.Consume(
		"notifications",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	return msgs, nil
}

func PublishNotification(message []byte) error {
	if ch == nil {
		return fmt.Errorf("rabbitmq channel is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := ch.PublishWithContext(ctx,
		"",
		"notifications",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}

	return nil
}
