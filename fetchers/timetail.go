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
	"github.com/combaine/combaine/worker"
)

func init() {
	worker.Register("timetail", NewTimetailFetcher)
}

type timetailFetcher struct {
	Port    int    `mapstructure:"timetail_port"`
	URL     string `mapstructure:"timetail_url"`
	Logname string `mapstructure:"logname"`
	Offset  int64  `mapstructure:"offset"`
}

// NewTimetailFetcher build new timetail fetcher
func NewTimetailFetcher(cfg repository.PluginConfig) (worker.Fetcher, error) {
	var fetcher timetailFetcher

	if err := decodeConfig(cfg, &fetcher); err != nil {
		return nil, err
	}
	if fetcher.Port == 0 {
		return nil, errors.New("timetail: Missing option port")
	}

	return &fetcher, nil
}

func (t *timetailFetcher) Fetch(ctx context.Context, task *worker.FetcherTask) ([]byte, error) {
	log := logrus.WithField("session", task.ID)

	period := t.Offset + (task.Frame.Current - task.Frame.Previous)

	url := fmt.Sprintf("http://%s:%d%s%s&time=%d", task.Target, t.Port, t.URL, t.Logname, period)
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("timetail: Context without deadline")
	}
	log.Infof("timetail: Requested URL: %s, timeout %v", url, deadline.Sub(time.Now()))

	resp, err := chttp.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	log.Infof("timetail: Result for URL %s: %d", url, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	return body, err
}
