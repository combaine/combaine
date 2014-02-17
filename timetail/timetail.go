package timetail

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/parsing"
)

func init() {
	parsing.Register("timetail", NewTimetail)
}

//{logname: nginx/access.log, timetail_port: 3132, timetail_url: '/timetail?log=',
func get(url string) ([]byte, error) {
	resp, err := http.Get(url)
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
	return get(url)
}

func NewTimetail(cfg map[string]interface{}) (t parsing.Fetcher, err error) {
	t = &Timetail{
		cfg: cfg,
	}
	return
}
