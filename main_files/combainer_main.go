package main

import (
	"flag"
	"log"
	"time"

	"github.com/noxiouz/Combaine/combainer"
	"github.com/noxiouz/Combaine/combainer/server"
)

const (
	defaultConfigsPath = "/etc/combaine"
	// DEFAULT_PERIOD     = 5
)

var (
	endpoint    string
	profiler    string
	logoutput   string
	loglevel    string
	ConfigsPath string
	period      uint
	active      bool
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&loglevel, "loglevel", "INFO", "loglevel (DEBUG|INFO|WARN|ERROR)")
	flag.StringVar(&ConfigsPath, "configspath", defaultConfigsPath, "path to root of configs")
	flag.UintVar(&period, "period", 5, "period of retrying new lock (sec)")
	flag.BoolVar(&active, "active", true, "enable a distribution of tasks")
}

func main() {
	flag.Parse()
	combainer.InitializeLogger(loglevel, logoutput)
	cfg := server.CombaineServerConfig{
		ConfigsPath:  ConfigsPath,
		Period:       time.Duration(period) * time.Second,
		RestEndpoint: endpoint,
		Active:       active,
	}

	cmb, err := server.NewCombainer(cfg)
	if err != nil {
		log.Fatal(err)
	}

	cmb.Serve()
}
