package main

import (
	"flag"
	"log"
	"net"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/combaine/combaine/aggregating"
	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/parsing"
	"github.com/combaine/combaine/rpc"

	_ "net/http/pprof"

	_ "github.com/combaine/combaine/fetchers/httpfetcher"
	_ "github.com/combaine/combaine/fetchers/rawsocket"
	_ "github.com/combaine/combaine/fetchers/timetail"
)

var cacher = servicecacher.NewCacher(servicecacher.NewService)

var endpoint string

func init() {
	flag.StringVar(&endpoint, "endpoint", ":10052", "endpoint")
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
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6061", nil))
	}()

	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(grpc.RPCCompressor(grpc.NewGZIPCompressor()), grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	rpc.RegisterWorkerServer(s, &server{})
	s.Serve(lis)
}
