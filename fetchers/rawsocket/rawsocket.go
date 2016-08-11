package rawsocket

import (
	"fmt"
	"io/ioutil"
	"net"

	"github.com/Combaine/Combaine/common/tasks"
	"github.com/Combaine/Combaine/parsing"
)

func init() {
	parsing.Register("tcpsocket", NewTcpSocketFetcher)
}

func get(host string, port string) (blob []byte, err error) {
	endpoint := net.JoinHostPort(host, port)
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return
	}
	blob, err = ioutil.ReadAll(conn)
	return
}

type tcpSocketFetcher struct {
	port interface{}
}

func (t *tcpSocketFetcher) Fetch(task *tasks.FetcherTask) (res []byte, err error) {
	return get(task.Target, fmt.Sprintf("%d", t.port))
}

func NewTcpSocketFetcher(cfg map[string]interface{}) (t parsing.Fetcher, err error) {

	port, ok := cfg["port"]
	if !ok {
		err = fmt.Errorf("port paramert is missing")
		return
	}

	t = &tcpSocketFetcher{
		port: port,
	}
	return
}
