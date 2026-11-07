package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"

	"notification-microservice/domain"
	mongogateway "notification-microservice/infra/mongo-gateway"
	rabbitmqgateway "notification-microservice/infra/rabbitmq-gateway"
	rabbitmqadapter "notification-microservice/internal/adapters/inbound/rabbitmq"
	mongoadapter "notification-microservice/internal/adapters/outbound/mongodb"
	notificationprovider "notification-microservice/internal/adapters/outbound/notification"
	applicationnotification "notification-microservice/internal/application/notification"
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

type appHandler func(w http.ResponseWriter, r *http.Request) error

func errorHandler(fn appHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			writeHTTPError(w, err)
		}
	}
}

func sendNotificationHandler(
	validate *validator.Validate,
	notificationService *service.NotificationService,
) appHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		defer r.Body.Close()

		var req NotificationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return requestError{message: "Invalid JSON body", status: http.StatusBadRequest}
		}

		if message := validateNotification(validate, req); message != "" {
			return requestError{message: message, status: http.StatusBadRequest}
		}

		notification := domain.Notification{
			UserID:    req.UserID,
			Message:   req.Message,
			Type:      req.Type,
			CreatedAt: time.Now().Format(time.RFC3339),
		}
		if err := notificationService.Send(notification); err != nil {
			return err
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "success"})
		return nil
	}
}

type requestError struct {
	message string
	status  int
}

func (e requestError) Error() string { return e.message }

func writeHTTPError(w http.ResponseWriter, err error) {
	var reqErr requestError
	if errors.As(err, &reqErr) {
		http.Error(w, reqErr.message, reqErr.status)
		return
	}

	if errors.Is(err, service.ErrNotificationDiscarded) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "notification discarded: user opted out",
		})
		return
	}

	if errors.Is(err, service.ErrOptOutCheck) {
		logrus.Errorf("Failed to check opt-out: %v", err)
		http.Error(w, "Failed to check notification preferences", http.StatusBadGateway)
		return
	}

	logrus.Errorf("Failed to send notification: %v", err)
	http.Error(w, "Internal Server Error", http.StatusInternalServerError)
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

func loadMongoConfig() (string, string) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	dbName := os.Getenv("MONGO_DATABASE")
	if dbName == "" {
		dbName = "notification_db"
	}
	return uri, dbName
}

func startRabbitMQ(url string) {
	err := rabbitmqgateway.InitRabbitMQ(url)
	if err != nil {
		logrus.Fatalf("Failed to initialize RabbitMQ: %v", err)
	}
}

func startMongoDB(uri, dbName string) {
	err := mongogateway.InitMongoDB(uri, dbName)
	if err != nil {
		logrus.Fatalf("Failed to initialize MongoDB: %v", err)
	}
}

func main() {
	startRabbitMQ(loadRabbitMQURL())
	defer rabbitmqgateway.Close()

	mongoURI, mongoDBName := loadMongoConfig()
	startMongoDB(mongoURI, mongoDBName)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mongogateway.Close(ctx)
	}()

	consumerDeliveries, err := rabbitmqgateway.ConsumeNotifications()
	if err != nil {
		logrus.Fatalf("Failed to start RabbitMQ consumer: %v", err)
	}

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	go func() {
		<-consumerCtx.Done()
		rabbitmqgateway.CloseConsumer()
	}()

	consumerRepository := mongoadapter.NewNotificationRepository(mongogateway.GetDatabase())
	notificationProcessor := applicationnotification.NewProcessor(consumerRepository, notificationprovider.NewProvider())
	notificationConsumer := rabbitmqadapter.NewConsumer(notificationProcessor, consumerDeliveries)
	go func() {
		if err := notificationConsumer.Start(consumerCtx); err != nil && !errors.Is(err, context.Canceled) {
			logrus.Errorf("Consumer exited with error: %v", err)
		}
	}()

	validate := newValidator()
	optOutChecker := repository.RandomOptOutChecker{}
	notificationRepository := repository.NewNotificationRepository(repository.RabbitMQNotificationPublisher{})
	notificationService := service.NewNotificationService(optOutChecker, notificationRepository)

	http.HandleFunc("/ping", pingHandler)
	http.Handle("/send-notification", errorHandler(sendNotificationHandler(validate, notificationService)))

	logrus.Info("Server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logrus.Fatalf("Server failed to start: %v", err)
	}
}
