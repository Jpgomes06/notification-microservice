package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"system-notification-consumer/consumer"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mockProvider struct {
	mu        sync.Mutex
	sentCalls []string
	fail      bool
}

func (m *mockProvider) Send(userID string, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("provider simulated error")
	}
	m.sentCalls = append(m.sentCalls, fmt.Sprintf("%s:%s", userID, message))
	return nil
}

func setupTestDB(t *testing.T) (*mongo.Client, *mongo.Collection, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://admin:secret@localhost:27017/?authSource=admin"))
	if err != nil {
		t.Skipf("Skipping integration test: MongoDB not available: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("Skipping integration test: MongoDB ping failed: %v", err)
	}

	collName := fmt.Sprintf("test_notifications_%d", time.Now().UnixNano())
	collection := client.Database("test_db").Collection(collName)

	cleanup := func() {
		_ = collection.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	}

	return client, collection, cleanup
}

func TestEnsureIndexes(t *testing.T) {
	_, collection, cleanup := setupTestDB(t)
	defer cleanup()

	err := consumer.EnsureIndexes(collection)
	if err != nil {
		t.Fatalf("expected EnsureIndexes to succeed, got %v", err)
	}

	cursor, err := collection.Indexes().List(context.Background())
	if err != nil {
		t.Fatalf("failed to list indexes: %v", err)
	}
	var indexes []bson.M
	if err := cursor.All(context.Background(), &indexes); err != nil {
		t.Fatalf("failed to decode indexes: %v", err)
	}

	foundCompound := false
	for _, idx := range indexes {
		if key, ok := idx["key"].(bson.M); ok {
			if key["status"] == int32(1) && key["send_at"] == int32(1) {
				foundCompound = true
				break
			}
		}
	}
	if !foundCompound {
		t.Errorf("expected compound index on status, send_at to exist")
	}
}

func TestProcessPendingNotifications_ImmediateAndFuture(t *testing.T) {
	_, collection, cleanup := setupTestDB(t)
	defer cleanup()

	now := time.Now().UTC()
	mock := &mockProvider{}

	// 1. Notificação vencida / envio imediato (send_at no passado)
	readyDoc := consumer.NotificationDocument{
		ID:        primitive.NewObjectID(),
		UserID:    "user-ready",
		Message:   "Ready message",
		Type:      "web",
		Status:    StatusPending,
		SendAt:    now.Add(-10 * time.Second),
		CreatedAt: now.Add(-10 * time.Second),
	}

	// 2. Notificação futura (send_at daqui a 1 hora)
	futureDoc := consumer.NotificationDocument{
		ID:        primitive.NewObjectID(),
		UserID:    "user-future",
		Message:   "Future message",
		Type:      "web",
		Status:    StatusPending,
		SendAt:    now.Add(1 * time.Hour),
		CreatedAt: now,
	}

	ctx := context.Background()
	_, err := collection.InsertMany(ctx, []interface{}{readyDoc, futureDoc})
	if err != nil {
		t.Fatalf("failed to insert test documents: %v", err)
	}

	// Executa um ciclo do Scheduler
	ProcessPendingNotifications(collection, mock)

	// Verifica documento que estava pronto
	var updatedReady consumer.NotificationDocument
	err = collection.FindOne(ctx, bson.M{"_id": readyDoc.ID}).Decode(&updatedReady)
	if err != nil {
		t.Fatalf("failed to find updated ready doc: %v", err)
	}
	if updatedReady.Status != StatusSent {
		t.Errorf("expected readyDoc status to be %s, got %s", StatusSent, updatedReady.Status)
	}
	if updatedReady.SendAt.Before(now) {
		t.Errorf("expected send_at to record the actual send time, got %v", updatedReady.SendAt)
	}

	// Verifica documento futuro
	var updatedFuture consumer.NotificationDocument
	err = collection.FindOne(ctx, bson.M{"_id": futureDoc.ID}).Decode(&updatedFuture)
	if err != nil {
		t.Fatalf("failed to find updated future doc: %v", err)
	}
	if updatedFuture.Status != StatusPending {
		t.Errorf("expected futureDoc status to remain %s, got %s", StatusPending, updatedFuture.Status)
	}
}

func TestProcessPendingNotifications_AtomicClaim_NoDuplicates(t *testing.T) {
	_, collection, cleanup := setupTestDB(t)
	defer cleanup()

	now := time.Now().UTC()
	mock := &mockProvider{}
	ctx := context.Background()

	// Insere 10 notificações prontas
	docs := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		docs[i] = consumer.NotificationDocument{
			ID:        primitive.NewObjectID(),
			UserID:    fmt.Sprintf("user-%d", i),
			Message:   fmt.Sprintf("Message %d", i),
			Type:      "web",
			Status:    StatusPending,
			SendAt:    now.Add(-time.Duration(i) * time.Second),
			CreatedAt: now,
		}
	}
	_, err := collection.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("failed to insert docs: %v", err)
	}

	// Simula 3 instâncias concorrentes do Scheduler rodando simultaneamente
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ProcessPendingNotifications(collection, mock)
		}()
	}
	wg.Wait()

	// Todas as 10 devem estar como 'sent'
	countSent, err := collection.CountDocuments(ctx, bson.M{"status": StatusSent})
	if err != nil {
		t.Fatalf("failed to count sent: %v", err)
	}
	if countSent != 10 {
		t.Errorf("expected 10 sent documents, got %d", countSent)
	}

	// Cada mensagem deve ter sido enviada exatamente 1 vez
	mock.mu.Lock()
	totalCalls := len(mock.sentCalls)
	mock.mu.Unlock()

	if totalCalls != 10 {
		t.Errorf("expected exactly 10 send calls without duplicates, got %d", totalCalls)
	}
}

func TestProcessPendingNotifications_ProviderFailure_RetriesAndDeadLetter(t *testing.T) {
	_, collection, cleanup := setupTestDB(t)
	defer cleanup()

	now := time.Now().UTC()
	failingMock := &mockProvider{fail: true}
	ctx := context.Background()

	doc := consumer.NotificationDocument{
		ID:        primitive.NewObjectID(),
		UserID:    "user-fail",
		Message:   "Fail message",
		Type:      "web",
		Status:    StatusPending,
		SendAt:    now.Add(-10 * time.Second),
		CreatedAt: now,
		Attempts:  0,
	}
	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		t.Fatalf("failed to insert doc: %v", err)
	}

	// Tentativa 1
	ProcessPendingNotifications(collection, failingMock)
	var after1 consumer.NotificationDocument
	_ = collection.FindOne(ctx, bson.M{"_id": doc.ID}).Decode(&after1)
	if after1.Status != StatusPending || after1.Attempts != 1 {
		t.Errorf("expected status=pending, attempts=1, got status=%s, attempts=%d", after1.Status, after1.Attempts)
	}
	if !after1.SendAt.After(now) {
		t.Errorf("expected send_at to have backoff in the future, got %v", after1.SendAt)
	}

	// Simula avanço do tempo para a tentativa 2
	_, _ = collection.UpdateOne(ctx, bson.M{"_id": doc.ID}, bson.M{"$set": bson.M{"send_at": time.Now().UTC().Add(-1 * time.Second)}})
	ProcessPendingNotifications(collection, failingMock)
	var after2 consumer.NotificationDocument
	_ = collection.FindOne(ctx, bson.M{"_id": doc.ID}).Decode(&after2)
	if after2.Status != StatusPending || after2.Attempts != 2 {
		t.Errorf("expected status=pending, attempts=2, got status=%s, attempts=%d", after2.Status, after2.Attempts)
	}

	// Simula avanço do tempo para a tentativa 3 (MaxAttempts = 3) -> deve ir para 'failed'
	_, _ = collection.UpdateOne(ctx, bson.M{"_id": doc.ID}, bson.M{"$set": bson.M{"send_at": time.Now().UTC().Add(-1 * time.Second)}})
	ProcessPendingNotifications(collection, failingMock)
	var after3 consumer.NotificationDocument
	_ = collection.FindOne(ctx, bson.M{"_id": doc.ID}).Decode(&after3)
	if after3.Status != StatusFailed || after3.Attempts != 3 {
		t.Errorf("expected status=failed, attempts=3, got status=%s, attempts=%d", after3.Status, after3.Attempts)
	}
	if after3.LastError == "" {
		t.Errorf("expected last_error to be recorded")
	}
}

func TestRecoverStuckNotifications(t *testing.T) {
	_, collection, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	oldTime := time.Now().UTC().Add(-10 * time.Minute)

	// Item preso com 1 tentativa -> deve voltar para 'pending'
	stuckRecoverable := consumer.NotificationDocument{
		ID:        primitive.NewObjectID(),
		UserID:    "user-stuck-1",
		Message:   "Stuck recoverable",
		Type:      "web",
		Status:    StatusProcessing,
		SendAt:    oldTime,
		CreatedAt: oldTime,
		UpdatedAt: oldTime,
		Attempts:  1,
	}

	// Item preso com MaxAttempts -> deve ir para 'failed'
	stuckExhausted := consumer.NotificationDocument{
		ID:        primitive.NewObjectID(),
		UserID:    "user-stuck-2",
		Message:   "Stuck exhausted",
		Type:      "web",
		Status:    StatusProcessing,
		SendAt:    oldTime,
		CreatedAt: oldTime,
		UpdatedAt: oldTime,
		Attempts:  MaxAttempts,
	}

	_, err := collection.InsertMany(ctx, []interface{}{stuckRecoverable, stuckExhausted})
	if err != nil {
		t.Fatalf("failed to insert stuck documents: %v", err)
	}

	recoverStuckNotifications(ctx, collection)

	var doc1, doc2 consumer.NotificationDocument
	_ = collection.FindOne(ctx, bson.M{"_id": stuckRecoverable.ID}).Decode(&doc1)
	_ = collection.FindOne(ctx, bson.M{"_id": stuckExhausted.ID}).Decode(&doc2)

	if doc1.Status != StatusPending {
		t.Errorf("expected stuckRecoverable status to be pending, got %s", doc1.Status)
	}

	if doc2.Status != StatusFailed {
		t.Errorf("expected stuckExhausted status to be failed, got %s", doc2.Status)
	}
}
