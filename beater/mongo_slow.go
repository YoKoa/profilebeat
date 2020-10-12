package beater

import (
	"context"
	"fmt"
	v "github.com/YoKoa/profilebeat/mongo"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sync"
	"time"
)

const (
	_delete  = "DELETE"
	_create  = "CREATE"
	_disable = "DISABLE"
	_enable  = "ENABLE"
)

type (
	Tailer struct {
		isRunning bool
		event     chan Event
		shutdown  chan struct{}
		runners   map[string]*Runner
	}

	Runner struct {
		db        string
		isRunning bool
		shutdown  chan struct{}
	}

	DBNameChecker struct {
		bt *profilebeat
		currentDB []string
		isRunning bool
		shutdown  chan struct{}
		event     chan Event
	}

	Event struct {
		Action string
		DB     string
	}
)

func NewRunner(db string) *Runner {
	return &Runner{
		db:        db,
		isRunning: false,
		shutdown:  make(chan struct{}),
	}
}

func (r *Runner) Run() {

}

func (r *Runner) Stop() {
	r.isRunning = false
}

func (r *Runner) Destory() {
	if !r.isRunning {
		close(r.shutdown)
	} else {
		r.Stop()
		close(r.shutdown)
	}
}

func NewDBNameChecker(bt *profilebeat) *DBNameChecker {
	return &DBNameChecker{
		bt: bt,
		currentDB: []string{},
		isRunning: false,
		shutdown:  make(chan struct{}),
		event:     make(chan Event, 1000),
	}
}
func (checker *DBNameChecker) Run() {
	ticker := time.NewTicker(30 * time.Minute)
	for {
		select {
		case <-ticker.C:
		case <-checker.shutdown:
			checker.isRunning = false
			break
		}
		ctx := context.Background()
		names, err := checker.bt.conn.ListDatabaseNames(ctx, primitive.M{})
		if err != nil {
			return
		}
		newDB := []string{}
		rmDB := []string{}
		for _, s := range newDB {

		}

		for _, s := range rmDB {

		}
	}
}

func (checker *DBNameChecker) Stop() {

}

func (checker *DBNameChecker) Destory() {

}

func NewTailer(event chan Event) *Tailer {
	return &Tailer{
		isRunning: false,
		event:     event,
		shutdown:  make(chan struct{}),
		runners:   make(map[string]*Runner),
	}
}

func (r *Tailer) Run() {
	for {
		select {
		case event, ok := <-r.event:
			if ok {
				switch {
				case event.Action == _create:
					if runner, exist := r.runners[event.DB]; exist {
						if runner.isRunning {
							log.Println("")
						}
					} else {
						newrunner := NewRunner(event.DB)
						r.runners[event.DB] = newrunner
						newrunner.Run()
					}
				case event.Action == _delete:
					targetrunner := r.runners[event.DB]
					targetrunner.Stop()
					targetrunner.Destory()
				}
			}
		case <-r.shutdown:
			r.Stop()
		}
	}
}

func (r *Tailer) Stop() {
	r.isRunning = false
	for _, runner := range r.runners {
		runner.Stop()
	}
}

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
			db := bt.dbs[i]
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				fmt.Println("db name is ", db)
				count, err := bt.conn.Database(db).Collection("system.profile").CountDocuments(ctx, primitive.M{})
				fmt.Println("profile doc num is ", count)
				if err != nil {
					fmt.Println("get profile count is err ", err)
					return
				}
				if count > 0 && db != "admin" { //>0 慢日志开启
					utc := time.Now().UTC()
					fmt.Println("utc time: ", utc)
					filter := bson.D{{"ts", bson.M{"$gte": utc}}}
					var doc v.SystemProfile
					opts := &options.FindOptions{}
					opts.SetCursorType(options.TailableAwait)
					fmt.Println("system.profile start....bb ", db)
					cursor, err := bt.conn.Database(db).Collection("system.profile").Find(ctx, filter, opts)
					if err != nil {
						fmt.Println("Err is ", err)
						panic(err)
					}

					for cursor.Next(ctx) {
						err := cursor.Decode(&doc)
						if err != nil {
							fmt.Println("cursor.Decode is ", err)
							return
						}
						fmt.Println("system.profile is ", doc)
					}
					if err := cursor.Err(); err != nil {
						fmt.Println("cursor.Err is ", err)
						return
					}
					//cursor.Close(ctx)
					// instantiate event
					event := beat.Event{
						Timestamp: time.Now(),
						Fields: common.MapStr{
							"@timestamp": common.Time(time.Now()),
							"type":       eventType,
							"doc":        doc,
							"ip":         "",
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
