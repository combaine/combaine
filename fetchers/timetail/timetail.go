package timetail

import (
	"fmt"
	"io/ioutil"
	"time"

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

type Timetail struct {
	cfg map[string]interface{}
}

func (t *Timetail) Fetch(task *tasks.FetcherTask) (res []byte, err error) {
	var period int
	if offset, ok := t.cfg["offset"]; ok {
		period = offset + (task.CurrTime - task.PrevTime)
	} else {
		period = task.CurrTime - task.PrevTime
	}

	url := fmt.Sprintf("http://%s:%d%s%s&time=%d",
		task.Target,
		t.cfg["timetail_port"],
		t.cfg["timetail_url"],
		t.cfg["logname"],
		period)

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
