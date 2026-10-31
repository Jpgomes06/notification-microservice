package notification

import (
	"github.com/sirupsen/logrus"

	"notification-microservice/domain"
)

type Provider struct{}

func NewProvider() *Provider {
	return &Provider{}
}

func (Provider) Send(notification domain.Notification) error {
	logrus.Infof("Fake notification sent (User: %s, Type: %s)", notification.UserID, notification.Type)
	return nil
}
