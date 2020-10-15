package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

// NewConnection
func NewConnection(urls string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(urls).SetMaxPoolSize(2))
	if err != nil {
		return nil,err
	}
	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Cannot connect to MongoDB: %s", err)
	}


	return client, nil
}

