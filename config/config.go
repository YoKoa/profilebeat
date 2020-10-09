// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import (
	"time"
)

type Config struct {
	Period time.Duration `config:"period"`
	Name string `config:"name"`
	Addrs string `config:"addrs"`
}

var DefaultConfig = Config{
	Period:   1 * time.Second,
	Name  :   "profilebeat",
	Addrs :   "localhost:27017",
}
