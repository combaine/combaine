package main

import (
	"flag"
	"log"
	"net"
	"os"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/combaine/combaine/aggregating"
	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/parsing"
	"github.com/combaine/combaine/rpc"

	_ "github.com/combaine/combaine/fetchers/httpfetcher"
	_ "github.com/combaine/combaine/fetchers/rawsocket"
	_ "github.com/combaine/combaine/fetchers/timetail"
)

var cacher = servicecacher.NewCacher(servicecacher.NewService)

var (
	endpoint  string
	logoutput string
)

func init() {
	flag.StringVar(&endpoint, "endpoint", ":10052", "endpoint")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.Parse()
}

type server struct{}

func (s *server) DoParsing(ctx context.Context, task *rpc.ParsingTask) (*rpc.ParsingResult, error) {
	return parsing.Do(ctx, task, cacher)
}

func (s *server) DoAggregating(ctx context.Context, task *rpc.AggregatingTask) (*rpc.AggregatingResult, error) {
	if err := aggregating.Do(ctx, task, cacher); err != nil {
		return nil, err
	}
	return new(rpc.AggregatingResult), nil
}

func main() {
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	rawFile, err := os.OpenFile(logoutput, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("failed to open logoutput %s: %v", logoutput, err)
	}
	var logger grpclog.Logger = log.New(rawFile, "", log.LstdFlags)
	grpclog.SetLogger(logger)
	s := grpc.NewServer(grpc.RPCCompressor(grpc.NewGZIPCompressor()), grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	rpc.RegisterWorkerServer(s, &server{})
	s.Serve(lis)
}
