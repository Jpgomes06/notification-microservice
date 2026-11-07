package inbound

import (
	"context"

	"notification-microservice/domain"
)

type NotificationProcessor interface {
	Process(ctx context.Context, notification domain.Notification) error
}
