package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"

	rabbitmqgateway "notification-microservice/infra/rabbitmq-gateway"
)

func pingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "pong"})
}

func sendNotificationHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()
	err := rabbitmqgateway.PublishNotification(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
