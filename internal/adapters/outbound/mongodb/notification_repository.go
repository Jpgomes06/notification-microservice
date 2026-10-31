package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"

	"notification-microservice/domain"
)

const NotificationCollectionName = "notification"

type NotificationRepository struct {
	collection *mongo.Collection
}

func NewNotificationRepository(db *mongo.Database) *NotificationRepository {
	return &NotificationRepository{collection: db.Collection(NotificationCollectionName)}
}

func (r *NotificationRepository) Save(ctx context.Context, notification domain.Notification) error {
	if r.collection == nil {
		return fmt.Errorf("mongo collection is not initialized")
	}

	if _, err := r.collection.InsertOne(ctx, notification); err != nil {
		return fmt.Errorf("insert notification into mongodb: %w", err)
	}

	return nil
}
