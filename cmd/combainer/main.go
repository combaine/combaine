package main

import (
	"flag"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	//_ "net/http/pprof"

	//_ "golang.org/x/net/trace"

	"github.com/combaine/combaine/combainer"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

var (
	endpoint    string
	profiler    string
	logoutput   string
	configsPath string
	active      bool
	tracing     bool
	version     bool
	loglevel    = logger.LogrusLevelFlag(logrus.InfoLevel)
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&configsPath, "configspath", repository.DefaultConfigsPath, "path to root of configs")
	flag.BoolVar(&active, "active", true, "enable a distribution of tasks")
	flag.BoolVar(&tracing, "trace", false, "enable tracing")
	flag.Var(&loglevel, "loglevel", "debug|info|warn|warning|error|panic in any case")
	flag.BoolVar(&version, "version", false, "print version and exit")
	flag.Parse()
	grpc.EnableTracing = tracing

	logger.InitializeLogger(loglevel.ToLogrusLevel(), logoutput)
	grpclog.SetLoggerV2(logger.NewLoggerV2WithVerbosity(0))
}

func main() {
	if version {
		fmt.Println(utils.GetVersionString())
		os.Exit(0)
	}

	log := logrus.WithField("source", "combainer/main.go")

	//go func() { log.Println(http.ListenAndServe("[::]:8001", nil)) }()

	err := repository.Init(configsPath)
	if err != nil {
		log.Fatalf("unable to initialize filesystemRepository: %s", err)
	}
	log.Info("filesystemRepository initialized")

	cfg := combainer.CombaineServerConfig{
		RestEndpoint: endpoint,
		Active:       active,
	}

	cmb, err := combainer.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Listen REST endoint on: %s", endpoint)
	if err = cmb.Serve(); err != nil {
		log.Fatal(err)
	}
}
