package mongo

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// NewConnection
func NewConnection(urls string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(urls))
	if err != nil {
		return nil,err
	}
	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Cannot connect to MongoDB: %s", err)
	}


	return client, nil
}

func Auto(client *mongo.Client, after primitive.Timestamp,db string,ctx context.Context){
	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			GetOpLogCursor(client,after,db,ctx)
		}
	}
}
func GetOpLogCursor(client *mongo.Client, after primitive.Timestamp,db string,ctx context.Context) {
	filter := bson.D{{"ts", bson.M{"$gt": after}}}
	var doc SystemProfile
	opts := &options.FindOptions{}
	opts.SetCursorType(options.TailableAwait)
	fmt.Println("system.profile start.... ",db)
	cursor, err := client.Database(db).Collection("system.profile").Find(ctx,filter,opts)
	if err!=nil{
		panic(err)
	}
	for cursor.Next(ctx){
		err := cursor.Decode(&doc)
		if err!=nil{
			return
		}
		fmt.Println("system.profile is ",doc)
	}

}

