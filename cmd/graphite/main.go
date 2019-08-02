package main

import (
	"context"
	"flag"
	"net"
	"time"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/senders"
	"github.com/combaine/combaine/senders/graphite"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/keepalive"
)

var (
	defaultFields = []string{
		"75_prc", "90_prc", "93_prc",
		"94_prc", "95_prc", "96_prc",
		"97_prc", "98_prc", "99_prc",
	}
	defaultGraphiteEndpoint = ":42000"
)

var (
	endpoint  string
	logoutput string
	tracing   bool
	loglevel  = logger.LogrusLevelFlag(logrus.InfoLevel)
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

type sender struct{}

// DoSend repack request and send points to graphite
func (*sender) DoSend(ctx context.Context, req *senders.SenderRequest) (*senders.SenderResponse, error) {
	log := logrus.WithFields(logrus.Fields{"session": req.Id})

	var cfg graphite.Config
	err := utils.Unpack(req.Config, &cfg)
	if err != nil {
		log.Errorf("Failed to unpack graphite task %s", err)
		return nil, err
	}
	task, err := senders.RepackSenderRequest(req)
	if err != nil {
		log.Errorf("Failed to repack sender request: %v", err)
		return nil, err
	}
	log.Debugf("Task: %v", task)

	if len(cfg.Fields) == 0 {
		cfg.Fields = defaultFields
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultGraphiteEndpoint
	}

	gCli, err := graphite.NewSender(&cfg, log)
	if err != nil {
		log.Errorf("Unexpected error %s", err)
		return nil, err
	}

	err = gCli.Send(task.Data, task.PrevTime)
	if err != nil {
		log.Errorf("Sending error %s", err)
		return nil, err
	}
	return &senders.SenderResponse{Response: "Ok"}, nil
}

func main() {
	log := logrus.WithField("source", "graphite/main.go")

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
	log.Infof("Register as gRPC server on: %s", endpoint)
	senders.RegisterSenderServer(s, &sender{})
	s.Serve(lis)
}
