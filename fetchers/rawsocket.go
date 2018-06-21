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
)

func init() {
	worker.Register("tcpsocket", NewTCPSocketFetcher)
}

// tcpSocketFetcher read data from raw tcp socket
type tcpSocketFetcher struct {
	Port    string `mapstructure:"port"`
	Timeout int    `mapstructure:"connection_timeout"`
	d       net.Dialer
}

// NewTCPSocketFetcher return rawsocket data fetcher
func NewTCPSocketFetcher(cfg repository.PluginConfig) (worker.Fetcher, error) {
	var f tcpSocketFetcher
	if err := decodeConfig(cfg, &f); err != nil {
		return nil, err
	}
	if f.Timeout <= 0 {
		f.Timeout = defaultTimeout
	}
	if f.Port == "" {
		return nil, errors.New("Missing option port")
	}
	f.d = net.Dialer{}
	return &f, nil
}

// Fetch dial with timeout and read data without timeout
func (t *tcpSocketFetcher) Fetch(task *common.FetcherTask) ([]byte, error) {
	ctx, closeF := context.WithTimeout(context.TODO(), time.Duration(t.Timeout)*time.Millisecond)
	defer closeF()
	conn, err := t.d.DialContext(ctx, "tcp", net.JoinHostPort(task.Target, t.Port))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(conn)
}
