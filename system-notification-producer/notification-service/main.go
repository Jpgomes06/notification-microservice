package main

import (
	"fmt"
	"net/http"

	"notification-service/handler"
	"notification-service/queue"
)

func main() {
	rabbitMQConnection := queue.ConnectRabbitMQ()
	defer rabbitMQConnection.Close()

	queue.CreateNotificationQueue(rabbitMQConnection)

	handler.SetupRoutes(rabbitMQConnection)

	fmt.Println("Notification Service running on port 8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}
