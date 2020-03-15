package main

import (
	"flag"
	"fmt"
	"os"

	"google.golang.org/grpc"

	//_ "net/http/pprof"

	//_ "golang.org/x/net/trace"

	"github.com/combaine/combaine/combainer"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

var (
	configsPath string
	active      bool
)

func init() {
	flag.StringVar(&configsPath, "configspath", repository.DefaultConfigsPath, "path to root of configs")
	flag.BoolVar(&active, "active", true, "enable a distribution of tasks")
	flag.Parse()
	grpc.EnableTracing = *utils.Flags.Tracing

	logger.InitializeLogger()
}

func main() {
	if *utils.Flags.Version {
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
		RestEndpoint: *utils.Flags.Endpoint,
		Active:       active,
	}

	cmb, err := combainer.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Listen REST endoint on: %s", *utils.Flags.Endpoint)
	if err = cmb.Serve(); err != nil {
		log.Fatal(err)
	}
}
