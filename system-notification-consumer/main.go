package main

import (
	"context"
	"fmt"
	"time"

	"system-notification-consumer/config"
	"system-notification-consumer/consumer"
	"system-notification-consumer/provider"
	"system-notification-consumer/scheduler"
)

func main() {
	rabbitMQConnection := config.ConnectRabbitMQ()
	defer rabbitMQConnection.Close()

	mongoClient := config.ConnectMongoDB()
	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		mongoClient.Disconnect(ctx)
	}()

	webProvider := provider.NewWebNotificationProvider()

	collection := mongoClient.Database("notifications_db").Collection("notifications")
	if err := consumer.EnsureIndexes(collection); err != nil {
		fmt.Println("Warning: Failed to ensure indexes:", err)
	}

	go consumer.ConsumeNotifications(
		rabbitMQConnection,
		mongoClient,
	)

	go scheduler.StartScheduler(
		mongoClient,
		webProvider,
	)

	fmt.Println("Notification Consumer running")

	select {}
}
