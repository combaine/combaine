package fetchers

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/combaine/combaine/common/chttp"
	"github.com/combaine/combaine/repository"
)

func init() {
	Register("http", NewHTTPFetcher)
}

type httpFetcher struct {
	Port int    `mapstructure:"port"`
	URI  string `mapstructure:"uri"`
}

// NewHTTPFetcher return http data fetcher
func NewHTTPFetcher(cfg repository.PluginConfig) (Fetcher, error) {
	var fetcher httpFetcher
	if err := decodeConfig(cfg, &fetcher); err != nil {
		return nil, err
	}
	if fetcher.URI == "" {
		fetcher.URI = "/"
	}
	if fetcher.Port == 0 {
		return nil, errors.New("httpfetcher: Missing option port")
	}

	return &fetcher, nil
}

func (t *httpFetcher) Fetch(ctx context.Context, task *FetcherTask) ([]byte, error) {
	log := logrus.WithField("session", task.ID)

	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("httpfetcher: Context without deadline")
	}

	url := fmt.Sprintf("http://%s:%d%s", task.Target, t.Port, t.URI)
	log.Infof("httpfetcher: Requested URL: %s, timeout %v", url, deadline.Sub(time.Now()))

	resp, err := chttp.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return body, err
}
