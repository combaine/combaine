package fetchers

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/worker"
	"golang.org/x/net/context/ctxhttp"
)

func init() {
	worker.Register("http", NewHTTPFetcher)
}

type httpFetcher struct {
	Port    int    `mapstructure:"port"`
	URI     string `mapstructure:"uri"`
	Timeout int    `mapstructure:"read_timeout"`
}

// NewHTTPFetcher return http data fetcher
func NewHTTPFetcher(cfg common.PluginConfig) (worker.Fetcher, error) {
	var fetcher httpFetcher
	if err := decodeConfig(cfg, &fetcher); err != nil {
		return nil, err
	}
	if fetcher.Timeout <= 0 {
		fetcher.Timeout = defaultTimeout
	}
	if fetcher.URI == "" {
		fetcher.URI = "/"
	}
	if fetcher.Port == 0 {
		return nil, errors.New("Missing option port")
	}

	return &fetcher, nil
}

func (t *httpFetcher) Fetch(task *common.FetcherTask) ([]byte, error) {
	logrus.Infof("%s HTTPFetcher config: %v", task.Id, t)
	url := fmt.Sprintf("http://%s:%d%s", task.Target, t.Port, t.URI)

	logrus.Infof("%s requested URL: %s, timeout %v", task.Id, url, t.Timeout)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.Timeout)*time.Millisecond)
	defer cancel()
	resp, err := ctxhttp.Get(ctx, nil, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return body, err
}
