package queue

import (
	"encoding/json"
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

const NotificationQueue = "queue-notification"

func ConnectRabbitMQ() *amqp.Connection {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	connection, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Connected to RabbitMQ")
	return connection
}

func CreateNotificationQueue(connection *amqp.Connection) {
	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	defer channel.Close()

	_, err = channel.QueueDeclare(
		NotificationQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Queue created:", NotificationQueue)
}

func PublishNotification(
	connection *amqp.Connection,
	notification interface{},
) error {
	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	message, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	err = channel.Publish(
		"",
		NotificationQueue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
	if err != nil {
		return err
	}

	fmt.Println("Notification published to RabbitMQ")
	return nil
}
