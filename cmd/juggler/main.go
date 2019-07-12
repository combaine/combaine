package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/senders"
	"github.com/combaine/combaine/senders/juggler"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/keepalive"
)

var (
	endpoint     string
	logoutput    string
	tracing      bool
	loglevel     = logger.LogrusLevelFlag(logrus.InfoLevel)
	senderConfig *juggler.SenderConfig
)

func init() {
	flag.StringVar(&endpoint, "endpoint", ":10052", "endpoint")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.BoolVar(&tracing, "trace", false, "enable tracing")
	flag.Var(&loglevel, "loglevel", "debug|info|warn|warning|error|panic in any case")
	flag.Parse()
	grpc.EnableTracing = tracing

	logger.InitializeLogger(loglevel.ToLogrusLevel(), logoutput)
	grpclog.SetLoggerV2(logger.NewLoggerV2WithVerbosity(0))
}

type sender struct {
}

func (*sender) DoSend(ctx context.Context, req *senders.SenderRequest) (*senders.SenderResponse, error) {
	log := logrus.WithFields(logrus.Fields{"session": req.Id})

	task, err := juggler.RepackSenderRequest(req, senderConfig)
	if err != nil {
		log.Errorf("Failed to repack sender request: %v", err)
		return nil, err
	}
	log.Debugf("Task.Data: %v", task.Data)
	jCli, err := juggler.NewSender(&task.Config, req.Id)
	if err != nil {
		log.Errorf("DoSend: Unexpected error %s", err)
		return nil, err
	}

	err = jCli.Send(ctx, task)
	if err != nil {
		log.Errorf("client.Send: %s", err)
		return nil, err
	}
	return &senders.SenderResponse{Response: "Ok"}, nil
}

func main() {
	//go func() { log.Println(http.ListenAndServe("[::]:8002", nil)) }()

	var err error
	senderConfig, err = juggler.GetSenderConfig()
	if err != nil {
		log.Fatalf("Failed to load sender config %s", err)
	}

	juggler.InitializeCache()

	err = repository.Init(juggler.GetConfigDir())
	if err != nil {
		log.Fatalf("unable to initialize filesystemRepository: %s", err)
	}
	logrus.Infof("filesystemRepository initialized")

	juggler.GlobalCache.TuneCache(
		senderConfig.CacheTTL,
		senderConfig.CacheCleanInterval,
		senderConfig.CacheCleanInterval*10,
	)
	juggler.InitEventsStore(&senderConfig.Store)

	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*128 /* 128 MB */),
		grpc.MaxSendMsgSize(1024*1024*128 /* 128 MB */),
		grpc.MaxConcurrentStreams(2000),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	senders.RegisterSenderServer(s, &sender{})
	s.Serve(lis)
}
