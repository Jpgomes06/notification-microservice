package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"notification-service/service"

	amqp "github.com/rabbitmq/amqp091-go"
)

type NotificationType string

const (
	TypeWeb   NotificationType = "web"
	TypePush  NotificationType = "push"
	TypeSMS   NotificationType = "sms"
	TypeEmail NotificationType = "email"
)

type SendNotificationRequest struct {
	UserID      string  `json:"user_id"`
	Message     string  `json:"message"`
	Type        string  `json:"type"`
	ScheduledAt *string `json:"scheduled_at,omitempty"`
}

type APIResponse struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
	Data    any    `json:"data,omitempty"`
}

func SetupRoutes(connection *amqp.Connection) {
	http.HandleFunc("POST /send-notification", func(w http.ResponseWriter, r *http.Request) {
		SendNotification(w, r, connection)
	})
}

func ParseAndValidateRequest(r *http.Request) (service.Notification, int, error) {
	var req SendNotificationRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return service.Notification{}, http.StatusBadRequest, fmt.Errorf("invalid request body")
	}

	if req.UserID == "" || req.Message == "" {
		return service.Notification{}, http.StatusBadRequest, fmt.Errorf("user_id and message are required")
	}

	if strings.TrimSpace(req.Type) == "" {
		return service.Notification{}, http.StatusBadRequest, fmt.Errorf("type is required")
	}

	notificationType := NotificationType(strings.ToLower(req.Type))
	switch notificationType {
	case TypeWeb, TypePush, TypeSMS, TypeEmail:
		// tipo válido
	default:
		return service.Notification{}, http.StatusBadRequest, fmt.Errorf("invalid notification type: supported types are web, push, sms, email")
	}

	var scheduledAt *time.Time
	if req.ScheduledAt != nil && strings.TrimSpace(*req.ScheduledAt) != "" {
		parsedTime, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.ScheduledAt))
		if err != nil {
			return service.Notification{}, http.StatusBadRequest, fmt.Errorf("invalid scheduled_at: must be in RFC3339 format with explicit timezone (e.g. 2026-09-17T14:30:00Z)")
		}

		now := time.Now().UTC()
		if parsedTime.Truncate(time.Minute).Equal(now.Truncate(time.Minute)) {
			return service.Notification{}, http.StatusBadRequest, fmt.Errorf("invalid scheduled_at: date/time must be later than the current time")
		}
		if parsedTime.Before(now) {
			return service.Notification{}, http.StatusBadRequest, fmt.Errorf("invalid scheduled_at: date/time cannot be in the past")
		}

		parsedTime = parsedTime.UTC()
		scheduledAt = &parsedTime
	}

	notification := service.Notification{
		UserID:      req.UserID,
		Message:     req.Message,
		Type:        string(notificationType),
		Status:      "pending",
		ScheduledAt: scheduledAt,
	}

	return notification, http.StatusOK, nil
}

func SendNotification(w http.ResponseWriter, r *http.Request, connection *amqp.Connection) {
	notification, statusCode, err := ParseAndValidateRequest(r)
	if err != nil {
		writeJSON(w, statusCode, APIResponse{
			Message: err.Error(),
			Status:  statusCode,
		})
		return
	}

	err = service.SendNotificationService(notification, connection)
	if err != nil {
		fmt.Println("Error in sendNotificationService:", err)
		writeJSON(w, http.StatusInternalServerError, APIResponse{
			Message: "error processing notification",
			Status:  http.StatusInternalServerError,
		})
		return
	}

	writeJSON(w, http.StatusCreated, APIResponse{
		Message: "notification created",
		Status:  http.StatusCreated,
		Data:    notification,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, response APIResponse) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		fmt.Println("Error encoding API response:", err)
	}
}
