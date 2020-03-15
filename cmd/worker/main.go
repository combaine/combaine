package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/keepalive"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/utils"
	"github.com/combaine/combaine/worker"
	"github.com/sirupsen/logrus"
	//_ "net/http/pprof"
	//_ "golang.org/x/net/trace"
)

func init() {
	flag.Parse()
	grpc.EnableTracing = *worker.Flags.Tracing

	lvl, err := logrus.ParseLevel(*worker.Flags.LogLevel)
	if err != nil {
		logrus.Fatalf("failed to parse loglevel: %v", err)
	}
	logger.InitializeLogger(lvl, *worker.Flags.LogOutput)
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
	if *worker.Flags.Version {
		fmt.Println(utils.GetVersionString())
		os.Exit(0)
	}

	log := logrus.WithField("source", "worker/main.go")

	//go func() { log.Println(http.ListenAndServe("[::]:8002", nil)) }()

	lis, err := net.Listen("tcp", *worker.Flags.Endpoint)
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
	log.Infof("Register as gRPC server on: %s", *worker.Flags.Endpoint)
	worker.RegisterWorkerServer(s, &server{})

	var stopCh = make(chan bool)
	defer close(stopCh)
	if err := worker.SpawnServices(stopCh); err != nil {
		log.Fatalf("Failed to spawn worker services: %v", err)
	}
	s.Serve(lis)
}
