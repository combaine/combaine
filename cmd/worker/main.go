package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/keepalive"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/worker"
	"github.com/sirupsen/logrus"

	//_ "net/http/pprof"

	//_ "golang.org/x/net/trace"

	_ "github.com/combaine/combaine/fetchers"
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

type server struct{}

func (s *server) DoParsing(ctx context.Context, task *worker.ParsingTask) (*worker.ParsingResult, error) {
	return worker.DoParsing(ctx, task)
}

func (s *server) DoAggregating(ctx context.Context, task *worker.AggregatingTask) (*worker.AggregatingResponse, error) {
	if err := worker.DoAggregating(ctx, task); err != nil {
		return nil, err
	}
	return new(worker.AggregatingResponse), nil
}

func main() {
	//go func() { log.Println(http.ListenAndServe("[::]:8002", nil)) }()

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
	worker.RegisterWorkerServer(s, &server{})

	if err := worker.SpawnAggregator(); err != nil {
		log.Fatalf("Failed to spawn worker services: %v", err)
	}
	s.Serve(lis)
}
