package notification

import (
	"context"
	"fmt"

	"notification-microservice/domain"
	"notification-microservice/internal/ports/outbound"
)

type Processor struct {
	repository outbound.NotificationRepository
	provider   outbound.NotificationProvider
}

func NewProcessor(repository outbound.NotificationRepository, provider outbound.NotificationProvider) *Processor {
	return &Processor{repository: repository, provider: provider}
}

func (p *Processor) Process(ctx context.Context, notification domain.Notification) error {
	if err := p.repository.Save(ctx, notification); err != nil {
		return fmt.Errorf("save notification: %w", err)
	}

	if err := p.provider.Send(notification); err != nil {
		return fmt.Errorf("send notification: %w", err)
	}

	return nil
}
