package service

import (
	"fmt"
	"math/rand"
	"time"

	"notification-service/queue"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Notification struct {
	UserID      string     `json:"user_id"`
	Message     string     `json:"message"`
	Type        string     `json:"type"`
	Status      string     `json:"status"`
	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
}

func SendNotificationService(
	notification Notification,
	connection *amqp.Connection,
) error {
	optOut, err := checkUserOptOut(notification.UserID)
	if err != nil {
		return err
	}

	fmt.Println("User opt-out:", optOut)

	if optOut {
		fmt.Println("User has opted out. Notification discarded.")
		return nil
	}

	fmt.Println("User has not opted out. Continue processing.")
	return queue.PublishNotification(connection, notification)
}
func checkUserOptOut(userID string) (bool, error) {
	fmt.Println("Checking opt-out for user:", userID)
	optOut := rand.Intn(2) == 1
	return optOut, nil
}
