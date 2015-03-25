package httpfetcher

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("http", NewHttpFetcher)
}

var (
	CONNECTION_TIMEOUT = 1000 * time.Millisecond
	RW_TIMEOUT         = 3000 * time.Millisecond
)

var HttpClient = httpclient.NewClientWithTimeout(CONNECTION_TIMEOUT, RW_TIMEOUT)

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

func (t *HttpFetcher) Fetch(task *tasks.FetcherTask) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d%s",
		task.Target,
		t.port,
		t.uri)

	logger.Infof("%s Requested URL: %s", task.Id, url)

	resp, err := HttpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}
