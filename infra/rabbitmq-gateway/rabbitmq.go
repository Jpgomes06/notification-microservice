package rabbitmqgateway

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	conn *amqp.Connection
	ch   *amqp.Channel
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
	log.Println("Successfully connected to RabbitMQ and declared queue")
	return nil
}

func Close() {
	if ch != nil {
		ch.Close()
	}
	if conn != nil {
		conn.Close()
	}
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
