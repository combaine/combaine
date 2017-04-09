package fetchers

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/parsing"
)

func init() {
	parsing.Register("http", NewHTTPFetcher)
}

type httpFetcher struct {
	Port    int    `mapstructure:"port"`
	URI     string `mapstructure:"uri"`
	Timeout int    `mapstructure:"read_timeout"`
}

// NewHTTPFetcher return http data fetcher
func NewHTTPFetcher(cfg common.PluginConfig) (parsing.Fetcher, error) {
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
	logger.Infof("%s HTTPFetcher config: %v", task.Id, t)
	url := fmt.Sprintf("http://%s:%d%s", task.Target, t.Port, t.URI)

	logger.Infof("%s requested URL: %s, timeout %v", task.Id, url, t.Timeout)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.Timeout)*time.Millisecond)
	resp, err := httpclient.Get(ctx, url)
	cancel()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}
