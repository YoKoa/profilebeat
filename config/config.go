// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import (
	"time"
)

type Config struct {
	Period    time.Duration `config:"period"`
	Name      string        `config:"name"`
	Addrs     string        `config:"addrs"`
	ClusterId string        `config:"clusterid"`
	Instance  string        `config:"instance"`
	Interval  time.Duration `config:"interval"`
}

var DefaultConfig = Config{
	Period:    1 * time.Second,
	Name:      "profileBeat",
	Addrs:     "localhost:27017",
	ClusterId: "",
	Instance:  "",
	Interval:  3 * time.Second,
}
