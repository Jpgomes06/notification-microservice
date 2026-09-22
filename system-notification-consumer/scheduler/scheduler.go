package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"system-notification-consumer/consumer"
	"system-notification-consumer/provider"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	StatusPending     = "pending"
	StatusProcessing  = "processing"
	StatusSent        = "sent"
	StatusFailed      = "failed"
	MaxAttempts       = 3
	ProcessingTimeout = 2 * time.Minute
	MaxBatchPerTick   = 50
)

func StartScheduler(
	mongoClient *mongo.Client,
	prov provider.NotificationProvider,
) {
	collection := mongoClient.
		Database("notifications_db").
		Collection("notifications")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Println("Scheduler started (atomic claim mode)")

	for range ticker.C {
		ProcessPendingNotifications(collection, prov)
	}
}

// ProcessPendingNotifications executa um ciclo de recuperação e claim atômico de notificações
func ProcessPendingNotifications(
	collection *mongo.Collection,
	prov provider.NotificationProvider,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	// 1. Recuperar notificações que possam ter ficado presas em "processing"
	recoverStuckNotifications(ctx, collection)

	// 2. Claim atômico e processamento dos itens elegíveis (send_at <= agora)
	processedCount := 0

	for processedCount < MaxBatchPerTick {
		now := time.Now().UTC()

		filter := bson.M{
			"status":  StatusPending,
			"send_at": bson.M{"$lte": now},
		}

		update := bson.M{
			"$set": bson.M{
				"status":     StatusProcessing,
				"updated_at": now,
			},
			"$inc": bson.M{
				"attempts": 1,
			},
		}

		findOptions := options.FindOneAndUpdate().SetReturnDocument(options.After)

		var notification consumer.NotificationDocument
		err := collection.FindOneAndUpdate(ctx, filter, update, findOptions).Decode(&notification)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				// Nenhuma notificação pendente pronta no momento
				break
			}
			log.Println("Error claiming pending notification:", err)
			break
		}

		// Documento reservado exclusivamente para esta instância
		processClaimedNotification(ctx, collection, notification, prov)
		processedCount++
	}

	if processedCount > 0 {
		fmt.Printf("Processed %d notification(s) in this scheduler cycle\n", processedCount)
	}
}

func processClaimedNotification(
	ctx context.Context,
	collection *mongo.Collection,
	notification consumer.NotificationDocument,
	defaultProv provider.NotificationProvider,
) {
	fmt.Printf(
		"Claimed notification - ID: %s | UserID: %s | Type: %s | Message: %s | SendAt: %s | Attempt: %d\n",
		notification.ID.Hex(),
		notification.UserID,
		notification.Type,
		notification.Message,
		notification.SendAt.Format(time.RFC3339),
		notification.Attempts,
	)

	activeProvider := defaultProv
	if activeProvider == nil {
		activeProvider = provider.GetProvider(notification.Type)
	}

	err := activeProvider.Send(notification.UserID, notification.Message)
	finishNow := time.Now().UTC()

	if err != nil {
		log.Printf("Error sending notification %s: %v\n", notification.ID.Hex(), err)

		nextStatus := StatusPending
		nextSendAt := notification.SendAt

		if notification.Attempts >= MaxAttempts {
			nextStatus = StatusFailed
		} else {
			// Backoff exponencial/linear para a próxima tentativa (5s, 10s...)
			backoff := time.Duration(notification.Attempts) * 5 * time.Second
			nextSendAt = finishNow.Add(backoff)
		}

		failUpdate := bson.M{
			"$set": bson.M{
				"status":     nextStatus,
				"send_at":    nextSendAt,
				"last_error": err.Error(),
				"updated_at": finishNow,
			},
		}

		_, updateErr := collection.UpdateOne(ctx, bson.M{"_id": notification.ID}, failUpdate)
		if updateErr != nil {
			log.Printf("Error updating failed notification %s: %v\n", notification.ID.Hex(), updateErr)
		}
		return
	}

	successUpdate := bson.M{
		"$set": bson.M{
			"status":     StatusSent,
			"send_at":    finishNow,
			"updated_at": finishNow,
		},
	}

	_, updateErr := collection.UpdateOne(ctx, bson.M{"_id": notification.ID}, successUpdate)
	if updateErr != nil {
		log.Printf("Error updating sent notification status for %s: %v\n", notification.ID.Hex(), updateErr)
		return
	}

	fmt.Printf("Notification %s successfully sent and marked as sent\n", notification.ID.Hex())
}

func recoverStuckNotifications(ctx context.Context, collection *mongo.Collection) {
	stuckCutoff := time.Now().UTC().Add(-ProcessingTimeout)

	// Falha definitiva se estourou o tempo e excedeu tentativas
	failedFilter := bson.M{
		"status":     StatusProcessing,
		"updated_at": bson.M{"$lte": stuckCutoff},
		"attempts":   bson.M{"$gte": MaxAttempts},
	}
	failedUpdate := bson.M{
		"$set": bson.M{
			"status":     StatusFailed,
			"last_error": "processing timed out and exceeded maximum retry attempts",
			"updated_at": time.Now().UTC(),
		},
	}
	resFailed, err := collection.UpdateMany(ctx, failedFilter, failedUpdate)
	if err != nil {
		log.Println("Error updating stuck expired notifications:", err)
	} else if resFailed.ModifiedCount > 0 {
		fmt.Printf("Marked %d expired stuck notification(s) as failed\n", resFailed.ModifiedCount)
	}

	// Retorna para pending caso ainda tenha tentativas disponíveis
	retryFilter := bson.M{
		"status":     StatusProcessing,
		"updated_at": bson.M{"$lte": stuckCutoff},
		"attempts":   bson.M{"$lt": MaxAttempts},
	}
	retryUpdate := bson.M{
		"$set": bson.M{
			"status":     StatusPending,
			"updated_at": time.Now().UTC(),
		},
	}
	resRetry, err := collection.UpdateMany(ctx, retryFilter, retryUpdate)
	if err != nil {
		log.Println("Error recovering stuck notifications to pending:", err)
	} else if resRetry.ModifiedCount > 0 {
		fmt.Printf("Recovered %d stuck notification(s) back to pending\n", resRetry.ModifiedCount)
	}
}
