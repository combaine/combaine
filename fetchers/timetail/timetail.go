package timetail

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("timetail", NewTimetail)
}

const CONNECTION_TIMEOUT = 1000
const RW_TIMEOUT = 3000

var HttpClient = httpclient.NewClientWithTimeout(
	time.Millisecond*CONNECTION_TIMEOUT,
	time.Millisecond*RW_TIMEOUT)

type Timetail struct {
	TimetailConfig
}

type TimetailConfig struct {
	Port        uint   `mapstructure:"timetail_port"`
	Url         string `mapstructure:"timetail_url"`
	Logname     string `mapstructure:"logname"`
	Offset      int64  `mapstructure:"offset"`
	ConnTimeout int    `mapstructure:"connection_timeout"`
	ReadTimeout int    `mapstructure:"read_timeout"`
}

func (t *Timetail) Fetch(task *tasks.FetcherTask) ([]byte, error) {
	period := t.Offset + (task.CurrTime - task.PrevTime)

	url := fmt.Sprintf("http://%s:%d%s%s&time=%d",
		task.Target,
		t.Port,
		t.Url,
		t.Logname,
		period)

	logger.Infof("%s Requested URL: %s", task.Id, url)

	var (
		resp *http.Response
		err  error
	)

	if t.TimetailConfig.ConnTimeout == CONNECTION_TIMEOUT && t.TimetailConfig.ReadTimeout == RW_TIMEOUT {
		resp, err = HttpClient.Get(url)
	} else {
		httpCli := httpclient.NewClientWithTimeout(
			time.Duration(t.TimetailConfig.ConnTimeout)*time.Millisecond,
			time.Duration(t.TimetailConfig.ReadTimeout)*time.Millisecond)
		httpCli.Transport.(*http.Transport).DisableKeepAlives = true
		resp, err = httpCli.Get(url)
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	logger.Infof("%s Result for URL %s: %d", task.Id, url, resp.StatusCode)

	body, err := ioutil.ReadAll(resp.Body)
	return body, nil
}

func NewTimetail(cfg map[string]interface{}) (t parsing.Fetcher, err error) {
	var (
		config         TimetailConfig
		decoder_config = mapstructure.DecoderConfig{
			// To allow decoder parses []uint8 as string
			WeaklyTypedInput: true,
			Result:           &config,
		}
	)

	decoder, err := mapstructure.NewDecoder(&decoder_config)
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(cfg)
	if err != nil {
		return nil, err
	}

	if config.ConnTimeout <= 0 {
		config.ConnTimeout = CONNECTION_TIMEOUT
	}

	if config.ReadTimeout <= 0 {
		config.ReadTimeout = RW_TIMEOUT
	}

	t = &Timetail{
		TimetailConfig: config,
	}
	return
}
