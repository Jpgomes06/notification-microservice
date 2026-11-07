package mongogateway

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	client   *mongo.Client
	database *mongo.Database
)

func InitMongoDB(uri, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)
	var err error
	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	database = client.Database(dbName)
	return nil
}

func GetDatabase() *mongo.Database {
	return database
}

func GetCollection(name string) *mongo.Collection {
	if database == nil {
		return nil
	}
	return database.Collection(name)
}

func Close(ctx context.Context) error {
	if client != nil {
		return client.Disconnect(ctx)
	}
	return nil
}
