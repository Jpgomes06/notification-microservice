package provider

import (
	"fmt"
	"strings"
)

type NotificationType string

const (
	TypeWeb   NotificationType = "web"
	TypePush  NotificationType = "push"
	TypeSMS   NotificationType = "sms"
	TypeEmail NotificationType = "email"
)

type WebNotificationProvider struct{}

func NewWebNotificationProvider() *WebNotificationProvider {
	return &WebNotificationProvider{}
}

func (p *WebNotificationProvider) Send(userID string, message string) error {
	fmt.Printf("[WEB] Notification saved/dispatched to user %s: %s\n", userID, message)
	return nil
}

// PushNotificationProvider simula o envio de Push Notifications (futuro)
type PushNotificationProvider struct{}

func (p *PushNotificationProvider) Send(userID string, message string) error {
	fmt.Printf("[PUSH (Simulado)] Notification sent to user %s: %s\n", userID, message)
	return nil
}

// SmsNotificationProvider simula o envio de SMS (futuro)
type SmsNotificationProvider struct{}

func (p *SmsNotificationProvider) Send(userID string, message string) error {
	fmt.Printf("[SMS (Simulado)] Notification sent to user %s: %s\n", userID, message)
	return nil
}

// EmailNotificationProvider simula o envio de Email (futuro)
type EmailNotificationProvider struct{}

func (p *EmailNotificationProvider) Send(userID string, message string) error {
	fmt.Printf("[EMAIL (Simulado)] Notification sent to user %s: %s\n", userID, message)
	return nil
}

// GetProvider resolve o provedor adequado com base no tipo da notificação
func GetProvider(notificationType string) NotificationProvider {
	switch strings.ToLower(notificationType) {
	case string(TypePush):
		return &PushNotificationProvider{}
	case string(TypeSMS):
		return &SmsNotificationProvider{}
	case string(TypeEmail):
		return &EmailNotificationProvider{}
	case string(TypeWeb):
		fallthrough
	default:
		return &WebNotificationProvider{}
	}
}
