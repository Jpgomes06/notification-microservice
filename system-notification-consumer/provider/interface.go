package provider

type NotificationProvider interface {
	Send(userID string, message string) error
}
