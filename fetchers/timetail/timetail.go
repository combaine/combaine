package timetail

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/noxiouz/Combaine/common/httpclient"
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
	Port    uint   `mapstructure:"timetail_port"`
	Url     string `mapstructure:"timetail_url"`
	Logname string `mapstructure:"logname"`
	Offset  int64  `mapstructure:"offset"`
}

//{logname: nginx/access.log, timetail_port: 3132, timetail_url: '/timetail?log=',
func get(url string) ([]byte, error) {
	resp, err := HttpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	return body, nil
}

func (t *Timetail) Fetch(task *tasks.FetcherTask) (res []byte, err error) {
	period := t.Offset + (task.CurrTime - task.PrevTime)

	url := fmt.Sprintf("http://%s:%d%s%s&time=%d",
		task.Target,
		t.Port,
		t.Url,
		t.Logname,
		period)

	log, err := parsing.LazyLoggerInitialization()
	if err != nil {
		return nil, err
	}
	log.Infof("%s Requested URL: %s", task.Id, url)

	return get(url)
}

func NewTimetail(cfg map[string]interface{}) (t parsing.Fetcher, err error) {
	var config TimetailConfig

	var decoder_config = mapstructure.DecoderConfig{
		// To allow decoder parses []uint8 as string
		WeaklyTypedInput: true,
		Result:           &config,
	}

	decoder, err := mapstructure.NewDecoder(&decoder_config)
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(cfg)
	if err != nil {
		return nil, err
	}

	t = &Timetail{
		TimetailConfig: config,
	}
	return
}
