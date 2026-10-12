package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"

	rabbitmqgateway "notification-microservice/infra/rabbitmq-gateway"
)

var validate = validator.New()

type NotificationRequest struct {
	UserID    string `json:"user_id" validate:"required"`
	Message   string `json:"message" validate:"required"`
	Type      string `json:"type" validate:"required"`
	CreatedAt string `json:"created_at,omitempty"`
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "pong"})
}

func sendNotificationHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req NotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}
	if err := validate.Struct(req); err != nil {
		http.Error(w, "Required fields are missing: user_id, message, and type.", http.StatusBadRequest)
		return
	}
	req.CreatedAt = time.Now().Format(time.RFC3339)
	body, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "Failed to prepare message", http.StatusInternalServerError)
		return
	}
	if err := rabbitmqgateway.PublishNotification(body); err != nil {
		http.Error(w, "Error sending to queue", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "success"})
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

	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/send-notification", sendNotificationHandler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		logrus.Fatalf("Server failed to start: %v", err)
	}
}
