package httpfetcher

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("http", NewHttpFetcher)
}

func get(url string) ([]byte, error) {
	resp, err := http.Get(url)
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
	return get(url)
}
