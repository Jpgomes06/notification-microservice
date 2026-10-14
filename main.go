package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"

	rabbitmqgateway "notification-microservice/infra/rabbitmq-gateway"
)

type NotificationType string

const (
	NotificationWeb   NotificationType = "web"
	NotificationPush  NotificationType = "push"
	NotificationSMS   NotificationType = "sms"
	NotificationEmail NotificationType = "email"
)

var supportedNotificationTypes = []NotificationType{
	NotificationWeb,
	NotificationPush,
	NotificationSMS,
	NotificationEmail,
}

type NotificationRequest struct {
	UserID  string `json:"user_id" validate:"required"`
	Message string `json:"message" validate:"required"`
	Type    string `json:"type" validate:"required"`
}

type NotificationMessage struct {
	UserID    string `json:"user_id"`
	Message   string `json:"message"`
	Type      string `json:"type"`
	CreatedAt string `json:"created_at"`
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "pong"})
}

func sendNotificationHandler(validate *validator.Validate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req NotificationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		if message := validateNotification(validate, req); message != "" {
			http.Error(w, message, http.StatusBadRequest)
			return
		}

		message := NotificationMessage{
			UserID:    req.UserID,
			Message:   req.Message,
			Type:      req.Type,
			CreatedAt: time.Now().Format(time.RFC3339),
		}

		body, err := json.Marshal(message)
		if err != nil {
			logrus.Errorf("Failed to prepare message: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		if err := rabbitmqgateway.PublishNotification(body); err != nil {
			logrus.Errorf("Failed to publish notification: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "success"})
	}
}

func newValidator() *validator.Validate {
	validate := validator.New()
	validate.RegisterTagNameFunc(func(field reflect.StructField) string {
		return field.Tag.Get("json")
	})
	return validate
}

func (t NotificationType) IsSupported() bool {
	return slices.Contains(supportedNotificationTypes, t)
}

func validateNotification(validate *validator.Validate, req NotificationRequest) string {
	err := validate.Struct(req)
	if err != nil {
		validationErrors, ok := err.(validator.ValidationErrors)
		if !ok {
			return "Invalid request"
		}

		fields := make([]string, 0, len(validationErrors))
		for _, fieldErr := range validationErrors {
			fields = append(fields, fieldErr.Field())
		}

		return "Required fields are missing: " + strings.Join(fields, ", ")
	}

	if !NotificationType(req.Type).IsSupported() {
		return fmt.Sprintf("Unsupported notification type %s", req.Type)
	}

	return ""
}

func loadRabbitMQURL() string {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://user:password@localhost:5672/"
	}
	return url
}

func startRabbitMQ(url string) {
	err := rabbitmqgateway.InitRabbitMQ(url)
	if err != nil {
		logrus.Fatalf("Failed to initialize RabbitMQ: %v", err)
	}
}

func main() {
	startRabbitMQ(loadRabbitMQURL())
	defer rabbitmqgateway.Close()

	validate := newValidator()

	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/send-notification", sendNotificationHandler(validate))

	if err := http.ListenAndServe(":8080", nil); err != nil {
		logrus.Fatalf("Server failed to start: %v", err)
	}
}
