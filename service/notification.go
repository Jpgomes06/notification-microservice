package service

import (
	"errors"
	"fmt"
	"time"

	"notification-microservice/domain"
)

var (
	ErrNotificationDiscarded = errors.New("notification discarded: user opted out")
	ErrOptOutCheck           = errors.New("opt-out check failed")
)

type (
	OptOutChecker interface {
		IsOptedOut(userID string) (bool, error)
	}

	NotificationRepository interface {
		Publish(notification any) error
	}
)

type NotificationService struct {
	optOutChecker OptOutChecker
	repository    NotificationRepository
}

func NewNotificationService(optOutChecker OptOutChecker, repository NotificationRepository) *NotificationService {
	return &NotificationService{optOutChecker: optOutChecker, repository: repository}
}

func (s *NotificationService) Send(notification domain.Notification) error {
	optedOut, err := s.optOutChecker.IsOptedOut(notification.UserID)
	if err != nil {
		return fmt.Errorf("%w for user %s: %v", ErrOptOutCheck, notification.UserID, err)
	}
	if optedOut {
		return ErrNotificationDiscarded
	}

	notification.CreatedAt = time.Now().Format(time.RFC3339)
	if err := s.repository.Publish(notification); err != nil {
		return fmt.Errorf("send notification: %w", err)
	}
	return nil
}
