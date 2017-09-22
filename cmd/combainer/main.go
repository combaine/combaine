package main

import (
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/combainer"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
)

var (
	endpoint    string
	profiler    string
	logoutput   string
	configsPath string
	period      uint
	active      bool
	tracing     bool
	loglevel    = logger.LogrusLevelFlag(logrus.InfoLevel)
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&configsPath, "configspath", common.DefaultConfigsPath, "path to root of configs")
	flag.UintVar(&period, "period", 5, "period of retrying new lock (sec)")
	flag.BoolVar(&active, "active", true, "enable a distribution of tasks")
	flag.BoolVar(&tracing, "trace", false, "enable tracing")
	flag.Var(&loglevel, "loglevel", "debug|info|warn|warning|error|panic in any case")
	flag.Parse()
	grpc.EnableTracing = tracing

	logger.InitializeLogger(loglevel.ToLogrusLevel(), logoutput)
	var grpcLogger grpclog.Logger = log.New(logrus.WithField("source", "grpc").Logger.Writer(), "", log.LstdFlags)
	grpclog.SetLogger(grpcLogger)
}

func main() {
	cfg := combainer.CombaineServerConfig{
		ConfigsPath:  configsPath,
		Period:       time.Duration(period) * time.Second,
		RestEndpoint: endpoint,
		Active:       active,
	}

	cmb, err := combainer.New(cfg)
	if err != nil {
		logrus.Fatal(err)
	}

	if err = cmb.Serve(); err != nil {
		logrus.Fatal(err)
	}
}
