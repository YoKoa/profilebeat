package beater

import (
	"context"
	"fmt"
	"github.com/YoKoa/profilebeat/mongo"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
	m "go.mongodb.org/mongo-driver/mongo"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/YoKoa/profilebeat/config"
)

// profilebeat configuration.
type profilebeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
	conn   *m.Client
	dbs    []string
	//TODO tailer
}

// New creates an instance of profilebeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}
	conn, err := mongo.NewConnection(c.Addrs)
	if err != nil {
		return nil, fmt.Errorf("Error connection mongodb: %v", err)
	}

	bt := &profilebeat{
		done:   make(chan struct{}),
		config: c,
		conn: conn,
	}
	return bt, nil
}

// Run starts profilebeat.
func (bt *profilebeat) Run(b *beat.Beat) error {
	logp.Info("profilebeat is running! Hit CTRL-C to stop it.")
	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(bt.config.Period)
	go bt.GetDBsList(ticker)
	go bt.mongoSlow(ticker)
	for {
		select {
		case <-bt.done:
			return nil
		}
	}


	//TODO 方式1: 使用游标tail数据
	//profileEvent := make(chan common.MapStr, 10000)
	//go func(events chan common.MapStr) {
	//	//todo Start tailer with configration
	//	//todo code(bt.tailer.run(events))
	//}(profileEvent)
	//
	//var pevent common.MapStr
	//
	//for {
	//	select {
	//	case <-bt.done:
	//		return nil
	//	case pevent = <-profileEvent:
	//
	//		event := beat.Event{
	//			Timestamp: time.Now(),
	//			Fields: common.MapStr{
	//				"value":   pevent["value"],
	//			},
	//		}
	//		bt.client.Publish(event)
	//	}
	//}
	//
	//
	////TODO 方式2: 每几秒就获取一次数据
	//ticker := time.NewTicker(bt.config.Period)
	//counter := 1
	//for {
	//	select {
	//	case <-bt.done:
	//		return nil
	//	case <-ticker.C:
	//	}
	//
	//	event := beat.Event{
	//		Timestamp: time.Now(),
	//		Fields: common.MapStr{
	//			"type":    b.Info.Name,
	//			"counter": counter,
	//		},
	//	}
	//	bt.client.Publish(event)
	//	logp.Info("Event sent")
	//	counter++
	//}

}



// Stop stops profilebeat.
func (bt *profilebeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *profilebeat) GetDBsList(ticker *time.Ticker) {
	for {
		select {

		case <-bt.done:
			return

		case <-ticker.C:

		}
		ctx :=context.Background()
		names, err := bt.conn.ListDatabaseNames(ctx, primitive.M{})
		if err != nil {
			return
		}
		bt.dbs=names
	}



}
