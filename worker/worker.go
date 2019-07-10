package worker

import (
	"time"

	"github.com/combaine/combaine/senders"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	grpc "google.golang.org/grpc"
)

var aggregatorConnection *grpc.ClientConn

// GetSenderClient return grpc client to locally spawned senders
func GetSenderClient(aType string) (senders.SenderClient, error) {
	return nil, errors.New("Not implemented yet")
}

// SpawnAggregator spawn subprocess with aggregator.py
func SpawnAggregator() error {
	options := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(1024*1024*128),
			grpc.MaxCallRecvMsgSize(1024*1024*128),
		),
	}
	//TODO cmd.Exec ...

	var attempts = 20
	for i := 0; i <= attempts; i++ {
		cc, err := grpc.Dial("[::1]:50051", options...)
		if err != nil {
			logrus.Warnf("Failed to dial aggregator.py: %v", err)
			time.Sleep(time.Second * 5)
			if i == attempts {
				return errors.Errorf("Failed to dial (%d times) aggregator.py: %v", attempts, err)
			}
		} else {
			aggregatorConnection = cc
		}
	}
	return nil
}
