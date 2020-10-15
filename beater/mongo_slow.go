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
	m "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const (
	_delete    = "DELETE"
	_create    = "CREATE"
	_disable   = "DISABLE"
	_enable    = "ENABLE"
	_eventType = "mongodb_slow"
)

type (
	Tailer struct {
		bt        *profilebeat
		isRunning bool
		event     chan Event
		shutdown  chan struct{}
		runners   map[string]*Runner
	}

	Runner struct {
		bt        *profilebeat
		db        string
		isRunning bool
		flag      bool
		shutdown  chan struct{}
	}

	DBNameChecker struct {
		bt        *profilebeat
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

func NewRunner(db string, bt *profilebeat) *Runner {
	return &Runner{
		bt:        bt,
		db:        db,
		isRunning: false,
		flag:      false,
		shutdown:  make(chan struct{}),
	}
}

func (r *Runner) Run() {
	defer func() {
		if r := recover(); r != nil {
			logp.Err("Panic error: %v", r)
		}
	}()
	logp.Info("profilebeat RUN %s system.profile.....", r.db)
	utc := time.Now().UTC()
	logp.Info("utc time: ", utc)
	ctx := context.Background()
	//判断当前数据库system.profile是否为空表
	if !r.flag {
		logp.Info("ProfileCount RUN %s .....", r.db)
		r.flag = ProfileCount(r.bt.conn, r.db, r.bt.interval)
	}

	opts := &options.FindOptions{}
	opts.SetCursorType(options.TailableAwait)
	filter := bson.D{{"ts", bson.M{"$gte": utc}}}
	logp.Info("system.profile db ..... ", r.db)
	cursor, err := r.bt.conn.Database(r.db).Collection("system.profile").Find(ctx, filter, opts)
	if err != nil {
		logp.Err("Failed to retrieve server status")
		return
	}
	logp.Info("current %s cursor id is %d", r.db, cursor.ID())
	for cursor.Next(ctx) {
		var doc v.SystemProfile
		logp.Info("system.profile start..... ")
		err := cursor.Decode(&doc)
		if err != nil {
			logp.Err("Failed to cursor.Decode")
		}
		// instantiate event
		event := beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"@timestamp": common.Time(time.Now()),
				"type":       _eventType,
				"doc":        doc,
				"instance":   r.bt.instance,
				"clusterId":  r.bt.clusterId,
			},
		}
		logp.Info("profilebeat event: ", event)
		// fire
		r.bt.client.Publish(event)
	}
	if err := cursor.Err(); err != nil {
		fmt.Println("cursor.Err is ", err)
		return
	}
	logp.Info("mongodb_slow Event sent", r.db)

}

func (r *Runner) Stop() {
	r.isRunning = false
}

func (r *Runner) Destroy() {
	if !r.isRunning {
		close(r.shutdown)
	} else {
		r.Stop()
		close(r.shutdown)
	}
}

func NewDBNameChecker(bt *profilebeat) *DBNameChecker {
	return &DBNameChecker{
		bt:        bt,
		currentDB: []string{},
		isRunning: false,
		shutdown:  make(chan struct{}),
		event:     make(chan Event, 1000),
	}
}
func (checker *DBNameChecker) Run() {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-checker.shutdown:
			checker.isRunning = false
			break
		}
		names, err := checker.bt.conn.ListDatabaseNames(context.Background(), primitive.M{})
		logp.Info("ListDatabaseNames is %+v", names)
		if err != nil {
			return
		}
		checker.isRunning = true

		if len(checker.currentDB) > 0 {
			//
			rmDB := Subtract(names, checker.currentDB)
			logp.Info("rmDB is %+v", rmDB)
			for _, s := range rmDB {
				dbName := Event{
					Action: _delete,
					DB:     s,
				}
				logp.Info("checker.event rm %s", s)
				checker.event <- dbName
			}
			newDB := Subtract(checker.currentDB, names)
			logp.Info("newDB is %+v", newDB)
			for _, s := range newDB {
				dbName := Event{
					Action: _create,
					DB:     s,
				}
				logp.Info("checker.event add %s", s)
				checker.event <- dbName
			}

		} else {
			logp.Info("first is ", names)
			for _, s := range names {
				dbName := Event{
					Action: _create,
					DB:     s,
				}
				logp.Info("checker.event add %s", s)
				checker.event <- dbName
			}

		}
		checker.currentDB = names
		logp.Info("currentDB : %+v", checker.currentDB)
	}
}

func (checker *DBNameChecker) Stop() {
	checker.isRunning = false
}

func (checker *DBNameChecker) Destroy() {
	if !checker.isRunning {
		close(checker.shutdown)
		close(checker.event)
	} else {
		checker.Stop()
		close(checker.shutdown)
		close(checker.event)
	}
}

func NewTailer(event chan Event, bt *profilebeat) *Tailer {
	return &Tailer{
		bt:        bt,
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
							logp.Info("runner :", event.DB)
						}
					} else {
						logp.Info("NewRunner :", event.DB)
						newRunner := NewRunner(event.DB, r.bt)
						r.runners[event.DB] = newRunner
						go newRunner.Run()
					}
				case event.Action == _delete:
					logp.Info("_delete :", event.DB)
					targetRunner := r.runners[event.DB]
					targetRunner.Stop()
					targetRunner.Destroy()
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

//轮训判断当前数据库system.profile是否为空
func ProfileCount(client *m.Client, dbName string, interval time.Duration) (flag bool) {
	ctx := context.Background()
	count, err := client.Database(dbName).Collection("system.profile").CountDocuments(ctx, primitive.M{})
	logp.Info("profile %s doc count is %d", dbName, count)
	if err != nil {
		logp.Err("Failed to system.profile count err")
		return false
	}
	for {
		if count == 0 {
			time.Sleep(interval)
			num, err := client.Database(dbName).Collection("system.profile").CountDocuments(ctx, primitive.M{})
			if err != nil {
				logp.Err("Failed to system.profile count err")
				return
			}
			count = num
			flag = false
			logp.Info("system.profile %s doc num is %d: ", dbName, count)
		} else {
			logp.Info("system.profile %s : ", dbName)
			flag = true
			break
		}
	}
	return flag
}

//获取当前mongodb数据库
func Subtract(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		_, ok := m[v]
		if !ok {
			nn = append(nn, v)
		}
	}
	return nn
}

func (bt *profilebeat) CheckPing() {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {

		case <-bt.done:
			return

		case <-ticker.C:

		}
		logp.Info("check ping start....")
		var flag bool
		if err := bt.conn.Ping(context.Background(), nil); err != nil {
			logp.Err("Ping error: %v", err)
			flag = true
		}
		if flag {
			for {
				if flag {
					if err := bt.conn.Ping(context.Background(), nil); err != nil {
						logp.Err("Ping error: %v", err)
						flag = true
					} else {
						flag = false
						break
					}
				}
			}

			if !flag {
				checker := NewDBNameChecker(bt)
				tailer := NewTailer(checker.event, bt)
				bt.checker = checker
				bt.tailer = tailer
				go bt.tailer.Run()
				logp.Info("tailer is running! ")
				go bt.checker.Run()
				logp.Info("checker is running! ")
			}
		}
	}

}
