package httpfetcher

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("http", NewHttpFetcher)
}

const (
	DEFAULT_CONNECTION_TIMEOUT = 1000
	DEFAULT_RW_TIMEOUT         = 3000
)

var (
	CONNECTION_TIMEOUT = DEFAULT_CONNECTION_TIMEOUT * time.Millisecond
	RW_TIMEOUT         = DEFAULT_RW_TIMEOUT * time.Millisecond
)

var HttpClient = httpclient.NewClientWithTimeout(CONNECTION_TIMEOUT, RW_TIMEOUT)

type HttpFetcher struct {
	port              interface{}
	uri               interface{}
	connectionTimeout int
	rwTimeout         int
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

	var (
		connTimeout int = DEFAULT_CONNECTION_TIMEOUT
		rwTimeout   int = DEFAULT_RW_TIMEOUT
	)

	if raw_val, ok := cfg["connection_timeout"]; ok {
		if val, ok := raw_val.(int); ok {
			connTimeout = val
		}
	}

	if raw_val, ok := cfg["read_timeout"]; ok {
		if val, ok := raw_val.(int); ok {
			rwTimeout = val
		}
	}

	t = &HttpFetcher{
		port:              port,
		uri:               uri,
		connectionTimeout: connTimeout,
		rwTimeout:         rwTimeout,
	}
	return
}

func (t *HttpFetcher) Fetch(task *tasks.FetcherTask) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d%s",
		task.Target,
		t.port,
		t.uri)

	logger.Infof("%s Requested URL: %s", task.Id, url)

	var (
		resp *http.Response
		err  error
	)
	if t.connectionTimeout == DEFAULT_CONNECTION_TIMEOUT && t.rwTimeout == DEFAULT_RW_TIMEOUT {
		resp, err = HttpClient.Get(url)
		if err != nil {
			return nil, err
		}
	} else {
		httpCli := httpclient.NewClientWithTimeout(
			time.Duration(t.connectionTimeout)*time.Millisecond,
			time.Duration(t.rwTimeout)*time.Millisecond)
		httpCli.Transport.(*http.Transport).DisableKeepAlives = true
		resp, err = httpCli.Get(url)
		if err != nil {
			return nil, err
		}
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}
