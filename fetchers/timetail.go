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
	worker.Register("timetail", NewTimetailFetcher)
}

type timetailFetcher struct {
	Port    int    `mapstructure:"timetail_port"`
	URL     string `mapstructure:"timetail_url"`
	Logname string `mapstructure:"logname"`
	Offset  int64  `mapstructure:"offset"`
	Timeout int    `mapstructure:"read_timeout"`
}

// NewTimetailFetcher build new timetail fetcher
func NewTimetailFetcher(cfg common.PluginConfig) (worker.Fetcher, error) {
	var fetcher timetailFetcher

	if err := decodeConfig(cfg, &fetcher); err != nil {
		return nil, err
	}
	if fetcher.Timeout <= 0 {
		fetcher.Timeout = defaultTimeout
	}
	if fetcher.Port == 0 {
		return nil, errors.New("Missing option port")
	}

	return &fetcher, nil
}

func (t *timetailFetcher) Fetch(task *common.FetcherTask) ([]byte, error) {
	period := t.Offset + (task.CurrTime - task.PrevTime)

	url := fmt.Sprintf("http://%s:%d%s%s&time=%d", task.Target, t.Port, t.URL, t.Logname, period)
	logrus.Infof("%s Requested URL: %s, timeout %v", task.Id, url, t.Timeout)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.Timeout)*time.Millisecond)
	defer cancel()
	resp, err := ctxhttp.Get(ctx, nil, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	logrus.Infof("%s Result for URL %s: %d", task.Id, url, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	return body, err
}
