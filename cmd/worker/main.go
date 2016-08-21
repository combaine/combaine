package main

import (
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	_ "github.com/combaine/combaine/fetchers/httpfetcher"
	_ "github.com/combaine/combaine/fetchers/rawsocket"
	_ "github.com/combaine/combaine/fetchers/timetail"
	"github.com/combaine/combaine/parsing"
	"github.com/combaine/combaine/rpc"
)

const (
	port = ":10000"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

// SayHello implements helloworld.GreeterServer
func (s *server) DoParsing(ctx context.Context, task *rpc.ParsingTask) (*rpc.ParsingResult, error) {
	return parsing.DoParsing(ctx, task)
}

func (s *server) DoAggregating(ctx context.Context, task *rpc.AggregatingTask) (*rpc.AggregatingResult, error) {
	return nil, nil
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
