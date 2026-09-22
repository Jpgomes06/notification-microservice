package config

import (
	"context"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectMongoDB() *mongo.Client {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://admin:secret@localhost:27017/?authSource=admin"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal("Erro ao criar cliente MongoDB:", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal("Não foi possível conectar ao MongoDB:", err)
	}
	log.Println("Conectado ao MongoDB com sucesso!")
	return client
}
