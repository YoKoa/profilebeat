package beater

import (
	"context"
	"fmt"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	v "github.com/YoKoa/profilebeat/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

func (bt *profilebeat) mongoSlow(ticker *time.Ticker) {
	eventType := "mongodb_slow"
	ctx := context.Background()
	for {
		select {

		case <-bt.done:
			return

		case <-ticker.C:

		}
		var wg sync.WaitGroup
		for i := 0; i < len(bt.dbs); i = i + 1 {
			db:=bt.dbs[i]
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				fmt.Println("db name is ",db)
				count, err :=bt.conn.Database(db).Collection("system.profile").CountDocuments(ctx, primitive.M{})
				fmt.Println("profile doc num is ",count)
				if err!=nil{
					fmt.Println("get profile count is err ",err)
					return
				}
				if count>0 && db!="admin"{//>0 慢日志开启
					utc := time.Now().UTC()
					fmt.Println("utc time: ",utc)
					filter := bson.D{{"ts", bson.M{"$gte": utc}}}
					var doc v.SystemProfile
					opts := &options.FindOptions{}
					opts.SetCursorType(options.TailableAwait)
					fmt.Println("system.profile start....bb ",db)
					cursor, err := bt.conn.Database(db).Collection("system.profile").Find(ctx,filter,opts)
					if err!=nil{
						fmt.Println("Err is ",err)
						panic(err)
					}

					for cursor.Next(ctx){
						err := cursor.Decode(&doc)
						if err!=nil{
							fmt.Println("cursor.Decode is ",err)
							return
						}
						fmt.Println("system.profile is ",doc)
					}
					if err := cursor.Err(); err != nil {
						fmt.Println("cursor.Err is ",err)
						return
					}
					//cursor.Close(ctx)
					// instantiate event
					event := beat.Event{
						Timestamp: time.Now(),
						Fields: common.MapStr{
							"@timestamp": common.Time(time.Now()),
							"type":       eventType,
							"doc":    doc,
							"ip":"",
						},
					}
					// fire
					bt.client.Publish(event)
					logp.Info("mongodb_slow Event sent")

				}
			}(i)
		}
		wg.Wait()
		bt.conn.Disconnect(ctx)

	}
}
