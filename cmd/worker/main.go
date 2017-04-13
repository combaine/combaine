package main

import (
	"flag"
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/combaine/combaine/aggregating"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/parsing"
	"github.com/combaine/combaine/rpc"

	_ "github.com/combaine/combaine/fetchers"
)

var cacher = cache.NewServiceCacher(cache.NewService)

var endpoint string

func init() {
	flag.StringVar(&endpoint, "endpoint", ":10052", "endpoint")
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
	flag.Parse()
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
