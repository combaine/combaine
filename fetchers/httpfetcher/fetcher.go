package httpfetcher

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("http", NewHttpFetcher)
}

const CONNECTION_TIMEOUT = 1000
const RW_TIMEOUT = 3000

var HttpClient = httpclient.NewClientWithTimeout(
	time.Millisecond*CONNECTION_TIMEOUT,
	time.Millisecond*RW_TIMEOUT)

func get(url string) ([]byte, error) {
	resp, err := HttpClient.Get(url)
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

func (t *HttpFetcher) Fetch(task *tasks.FetcherTask) (res []byte, err error) {
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
