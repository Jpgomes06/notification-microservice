package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"notification-microservice/domain"
	rabbitmqadapter "notification-microservice/internal/adapters/inbound/rabbitmq"
)

type mockNotificationSaver struct {
	saved []domain.Notification
	err   error
}

type mockProcessor struct {
	save func(context.Context, domain.Notification) error
}

func (m mockProcessor) Process(ctx context.Context, notification domain.Notification) error {
	return m.save(ctx, notification)
}

func (m *mockNotificationSaver) Save(ctx context.Context, notification domain.Notification) error {
	if m.err != nil {
		return m.err
	}
	m.saved = append(m.saved, notification)
	return nil
}

func TestConsumer_ProcessMessage_Success(t *testing.T) {
	mockSaver := &mockNotificationSaver{}
	c := rabbitmqadapter.NewConsumer(mockProcessor{save: mockSaver.Save}, nil)

	notification := domain.Notification{
		UserID:    "user-123",
		Message:   "Test notification",
		Type:      "email",
		CreatedAt: time.Now().Format(time.RFC3339),
	}
	body, err := json.Marshal(notification)
	if err != nil {
		t.Fatalf("Failed to marshal notification: %v", err)
	}

	msg := amqp.Delivery{
		Body: body,
	}

	err = c.ProcessMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(mockSaver.saved) != 1 {
		t.Fatalf("Expected 1 saved notification, got: %d", len(mockSaver.saved))
	}

	saved := mockSaver.saved[0]
	if saved.UserID != notification.UserID || saved.Message != notification.Message || saved.Type != notification.Type {
		t.Errorf("Saved notification mismatch: %+v vs %+v", saved, notification)
	}
}

func TestConsumer_ProcessMessage_InvalidJSON(t *testing.T) {
	mockSaver := &mockNotificationSaver{}
	c := rabbitmqadapter.NewConsumer(mockProcessor{save: mockSaver.Save}, nil)

	msg := amqp.Delivery{
		Body: []byte("invalid json"),
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("Expected error for invalid json, got nil")
	}

	if len(mockSaver.saved) != 0 {
		t.Fatalf("Expected 0 saved notifications, got: %d", len(mockSaver.saved))
	}
}

func TestConsumer_ProcessMessage_SaverError(t *testing.T) {
	mockSaver := &mockNotificationSaver{
		err: errors.New("database connection failed"),
	}
	c := rabbitmqadapter.NewConsumer(mockProcessor{save: mockSaver.Save}, nil)

	notification := domain.Notification{
		UserID:  "user-999",
		Message: "Fail test",
		Type:    "push",
	}
	body, _ := json.Marshal(notification)

	msg := amqp.Delivery{
		Body: body,
	}

	err := c.ProcessMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("Expected error from saver, got nil")
	}
}
