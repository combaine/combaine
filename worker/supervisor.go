package worker

import (
	"time"

	grpc "google.golang.org/grpc"
)

var aggregatorConnection *grpc.ClientConn

// SpawnAggregator spawn subprocess with aggregator.py
func SpawnAggregator() error {
	options := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(1024*1024*128),
			grpc.MaxCallRecvMsgSize(1024*1024*128),
		),
	}
	// TODO cmd.Exec ...

	var attempts = 20
	for {
		cc, err := grpc.Dial(addr, options...)
		if err != nil {
			log.Warnf("Failed to dial aggregator.py: %v", err)
			attempts--
			time.Sleep(time.Second * 5)
		} else {
			aggregatorConnection = cc
		}
	}
}
