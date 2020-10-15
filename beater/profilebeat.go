package beater

import (
	"fmt"
	"github.com/YoKoa/profilebeat/config"
	"github.com/YoKoa/profilebeat/mongo"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	m "go.mongodb.org/mongo-driver/mongo"
	"time"
)

// profilebeat configuration.
type profilebeat struct {
	done      chan struct{}
	config    config.Config
	client    beat.Client
	conn      *m.Client
	clusterId string
	instance  string
	dbs       []string
	interval  time.Duration

	tailer  *Tailer
	checker *DBNameChecker
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
		done:      make(chan struct{}),
		config:    c,
		conn:      conn,
		clusterId: c.ClusterId,
		instance:  c.Instance,
		interval: c.Interval,

	}
	checker := NewDBNameChecker(bt)
	tailer := NewTailer(checker.event, bt)
	bt.checker = checker
	bt.tailer = tailer
	return bt, nil
}

// Run starts profilebeat.
func (bt *profilebeat) Run(b *beat.Beat) error {
	logp.Info("profilebeat is running! Hit CTRL-C to stop it.")
	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		logp.Err("Failed to retrieve server status")
		return err
	}

	go bt.tailer.Run()
	logp.Info("tailer is running! ")
	go bt.checker.Run()
	logp.Info("checker is running! ")
	//
    go bt.CheckPing()
	logp.Info("CheckPing is running! ")
	for {
		select {
		case <-bt.done:
			return nil
		}
	}
}

// Stop stops profilebeat.
func (bt *profilebeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
