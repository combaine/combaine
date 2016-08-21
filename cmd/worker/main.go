package main

import (
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/combaine/combaine/aggregating"
	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/parsing"
	"github.com/combaine/combaine/rpc"

	_ "github.com/combaine/combaine/fetchers/httpfetcher"
	_ "github.com/combaine/combaine/fetchers/rawsocket"
	_ "github.com/combaine/combaine/fetchers/timetail"
)

var cacher = servicecacher.NewCacher()

const (
	port = ":10000"
)

type server struct{}

func (s *server) DoParsing(ctx context.Context, task *rpc.ParsingTask) (*rpc.ParsingResult, error) {
	return parsing.Do(ctx, task)
}

func (s *server) DoAggregating(ctx context.Context, task *rpc.AggregatingTask) (*rpc.AggregatingResult, error) {
	if err := aggregating.Do(ctx, task, cacher); err != nil {
		return nil, err
	}
	return new(rpc.AggregatingResult), nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	rpc.RegisterWorkerServer(s, &server{})
	s.Serve(lis)
}
