package main

import (
	"flag"
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/rpc"
	"github.com/combaine/combaine/worker"

	_ "github.com/combaine/combaine/fetchers"
)

var (
	cacher    = cache.NewServiceCacher(cache.NewServiceWithLocalLogger)
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
	var grpcLogger grpclog.Logger = log.New(logrus.WithField("source", "grpc").Logger.Writer(), "", log.LstdFlags)
	grpclog.SetLogger(grpcLogger)
}

type server struct{}

func (s *server) DoParsing(ctx context.Context, task *rpc.ParsingTask) (*rpc.ParsingResult, error) {
	return worker.DoParsing(ctx, task, cacher)
}

func (s *server) DoAggregating(ctx context.Context, task *rpc.AggregatingTask) (*rpc.AggregatingResult, error) {
	if err := worker.DoAggregating(ctx, task, cacher); err != nil {
		return nil, err
	}
	return new(rpc.AggregatingResult), nil
}

func main() {
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(
		grpc.MaxMsgSize(1024*1024*128 /* 128 MB */),
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()),
	)
	rpc.RegisterWorkerServer(s, &server{})
	s.Serve(lis)
}
