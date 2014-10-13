package timetail

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("timetail", NewTimetail)
}

const CONNECTION_TIMEOUT = 1000
const RW_TIMEOUT = 3000

//{logname: nginx/access.log, timetail_port: 3132, timetail_url: '/timetail?log=',
func get(url string) ([]byte, error) {
	client := httpclient.NewClientWithTimeout(
		time.Millisecond*CONNECTION_TIMEOUT,
		time.Millisecond*RW_TIMEOUT)
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return body, nil
}

type Timetail struct {
	cfg map[string]interface{}
}

func (t *Timetail) Fetch(task *common.FetcherTask) (res []byte, err error) {
	url := fmt.Sprintf("http://%s:%d%s%s&time=%d",
		task.Target,
		t.cfg["timetail_port"],
		t.cfg["timetail_url"],
		t.cfg["logname"],
		task.EndTime-task.StartTime)

	log, err := parsing.LazyLoggerInitialization()
	if err != nil {
		return nil, err
	}
	log.Infof("%s Requested URL: %s", task.Id, url)

	return get(url)
}

func NewTimetail(cfg map[string]interface{}) (t parsing.Fetcher, err error) {
	t = &Timetail{
		cfg: cfg,
	}
	return
}
