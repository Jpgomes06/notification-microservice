package outbound

import (
	"context"

	"notification-microservice/domain"
)

type NotificationRepository interface {
	Save(ctx context.Context, notification domain.Notification) error
}
