package fetchers

import (
	"context"
	"errors"
	"io/ioutil"
	"net"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/worker"
	"github.com/sirupsen/logrus"
)

func init() {
	worker.Register("tcpsocket", NewTCPSocketFetcher)
}

// tcpSocketFetcher read data from raw tcp socket
type tcpSocketFetcher struct {
	Port string `mapstructure:"port"`
	d    net.Dialer
}

// NewTCPSocketFetcher return rawsocket data fetcher
func NewTCPSocketFetcher(cfg repository.PluginConfig) (worker.Fetcher, error) {
	var f tcpSocketFetcher
	if err := decodeConfig(cfg, &f); err != nil {
		return nil, err
	}
	if f.Port == "" {
		return nil, errors.New("rawsocket: Missing option port")
	}
	f.d = net.Dialer{}
	return &f, nil
}

// Fetch dial with timeout and read data without timeout
func (t *tcpSocketFetcher) Fetch(ctx context.Context, task *common.FetcherTask) ([]byte, error) {
	log := logrus.WithField("session", task.Id)

	address := net.JoinHostPort(task.Target, t.Port)
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("rawsocket: Context without deadline")
	}
	log.Infof("rawsocket: Requested Address: %s, timeout %v", address, deadline.Sub(time.Now()))
	conn, err := t.d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(conn)
}
