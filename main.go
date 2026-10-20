package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"

	rabbitmqgateway "notification-microservice/infra/rabbitmq-gateway"
	"notification-microservice/repository"
	"notification-microservice/service"
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

func pingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
}

func sendNotificationHandler(
	validate *validator.Validate,
	notificationService *service.NotificationService,
) http.HandlerFunc {
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

		err := notificationService.Send(req.UserID, req.Message, req.Type)

		if errors.Is(err, service.ErrNotificationDiscarded) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{
				"message": "notification discarded: user opted out",
			})
			return
		}

		if errors.Is(err, service.ErrOptOutCheck) {
			logrus.Errorf("Failed to check opt-out for user %s: %v", req.UserID, err)
			http.Error(w, "Failed to check notification preferences", http.StatusBadGateway)
			return
		}

		if err != nil {
			logrus.Errorf("Failed to send notification for user %s: %v", req.UserID, err)
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
	optOutChecker := repository.RandomOptOutChecker{}
	notificationRepository := repository.NewNotificationRepository(repository.RabbitMQNotificationPublisher{})
	notificationService := service.NewNotificationService(optOutChecker, notificationRepository)

	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/send-notification", sendNotificationHandler(validate, notificationService))

	if err := http.ListenAndServe(":8080", nil); err != nil {
		logrus.Fatalf("Server failed to start: %v", err)
	}
}
