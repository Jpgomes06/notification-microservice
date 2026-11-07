package outbound

import "notification-microservice/domain"

type NotificationProvider interface {
	Send(notification domain.Notification) error
}
