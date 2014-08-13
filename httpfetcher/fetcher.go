package httpfetcher

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("http", NewHttpFetcher)
}

const CONNECTION_TIMEOUT = 1000
const RW_TIMEOUT = 3000

func timeoutDialer(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, cTimeout)
		if err != nil {
			return nil, err
		}
		conn.SetDeadline(time.Now().Add(rwTimeout))
		return conn, nil
	}
}

// HTTP Client, which has connection and r/w timeouts
func NewClientWithTimeout(connectTimeout time.Duration, rwTimeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Dial: timeoutDialer(connectTimeout, rwTimeout),
		},
	}
}

func get(url string) ([]byte, error) {

	// resp, err := http.Get(url)
	client := NewClientWithTimeout(
		time.Millisecond*CONNECTION_TIMEOUT,
		time.Millisecond*RW_TIMEOUT)
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return body, nil
}

type HttpFetcher struct {
	port interface{}
	uri  interface{}
}

func NewHttpFetcher(cfg map[string]interface{}) (t parsing.Fetcher, err error) {
	port, ok := cfg["port"]
	if !ok {
		err = fmt.Errorf("Missing option port")
		return
	}

	uri, ok := cfg["uri"]
	if !ok {
		uri = "/"
	}

	t = &HttpFetcher{
		port: port,
		uri:  uri,
	}
	return
}

func (t *HttpFetcher) Fetch(task *common.FetcherTask) (res []byte, err error) {
	url := fmt.Sprintf("http://%s:%d%s",
		task.Target,
		t.port,
		t.uri)

	log, err := parsing.LazyLoggerInitialization()
	if err != nil {
		return nil, err
	}
	log.Infof("%s Requested URL: %s", task.Id, url)
	return get(url)
}
