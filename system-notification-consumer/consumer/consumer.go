package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type NotificationDocument struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID      string             `bson:"user_id" json:"user_id"`
	Message     string             `bson:"message" json:"message"`
	Type        string             `bson:"type" json:"type"`
	Status      string             `bson:"status" json:"status"`
	SendAt      time.Time          `bson:"send_at" json:"send_at"`
	ScheduledAt *time.Time         `bson:"scheduled_at,omitempty" json:"scheduled_at,omitempty"`
	CreatedAt   time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt   time.Time          `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
	Attempts    int                `bson:"attempts" json:"attempts"`
	LastError   string             `bson:"last_error,omitempty" json:"last_error,omitempty"`
}

type queuedNotification struct {
	UserID      string     `json:"user_id"`
	Message     string     `json:"message"`
	Type        string     `json:"type"`
	Status      string     `json:"status"`
	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
}

func EnsureIndexes(collection *mongo.Collection) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "status", Value: 1},
			{Key: "send_at", Value: 1},
		},
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("error creating index on notifications: %w", err)
	}

	log.Println("MongoDB compound index (status, send_at) ensured")
	return nil
}

func ConsumeNotifications(
	connection *amqp.Connection,
	mongoClient *mongo.Client,
) {
	for {
		err := consumeMessages(connection, mongoClient)

		if err != nil {
			log.Println("Consumer stopped:", err)
			log.Println("Restarting consumer in 5 seconds...")
			time.Sleep(5 * time.Second)
		}
	}
}

func consumeMessages(
	connection *amqp.Connection,
	mongoClient *mongo.Client,
) error {
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("error opening RabbitMQ channel: %w", err)
	}
	defer channel.Close()

	messages, err := channel.Consume(
		"queue-notification",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error consuming messages: %w", err)
	}

	collection := mongoClient.
		Database("notifications_db").
		Collection("notifications")

	fmt.Println("Waiting for notifications...")

	for message := range messages {
		fmt.Println("Notification received:", string(message.Body))

		var queued queuedNotification

		if err := json.Unmarshal(message.Body, &queued); err != nil {
			log.Println("Error parsing notification:", err)

			if err := message.Ack(false); err != nil {
				log.Println("Error acknowledging invalid message:", err)
			}

			continue
		}

		now := time.Now().UTC()
		sendAt := now
		if queued.ScheduledAt != nil {
			sendAt = queued.ScheduledAt.UTC()
		}
		status := queued.Status
		if status == "" {
			status = "pending"
		}
		typeName := queued.Type
		if typeName == "" {
			typeName = "web"
		}
		doc := NotificationDocument{
			UserID: queued.UserID, Message: queued.Message, Type: typeName,
			Status: status, SendAt: sendAt, ScheduledAt: queued.ScheduledAt,
			CreatedAt: now, UpdatedAt: now,
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		_, err := collection.InsertOne(ctx, doc)

		cancel()

		if err != nil {
			log.Println("Error saving to MongoDB:", err)
			continue
		}

		if err := message.Ack(false); err != nil {
			log.Println("Error acknowledging message:", err)
			continue
		}

		fmt.Println("Notification saved to DB and acknowledged")
	}

	return fmt.Errorf("RabbitMQ message channel closed")
}
