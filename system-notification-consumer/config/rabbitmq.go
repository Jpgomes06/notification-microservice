package config

import (
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ConnectRabbitMQ() *amqp.Connection {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	for {
		connection, err := amqp.Dial(url)
		if err == nil {
			fmt.Println("Connected to RabbitMQ")
			return connection
		}

		log.Println("Error connecting to RabbitMQ:", err)
		log.Println("Retrying RabbitMQ connection in 5 seconds...")

		time.Sleep(5 * time.Second)
	}
}
