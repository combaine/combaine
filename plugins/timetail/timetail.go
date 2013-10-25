package timetail

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type InputData struct {
	Logname       string "logname"
	Timetail_port int    "timetail_port"
	Timetail_url  string "timetail_url"
	Host          string "host"
	StartTime     int    "Time"
}

func (inp *InputData) AsString() string {
	return fmt.Sprintf("http://%s:%d%s%s&time=%d", inp.Host, inp.Timetail_port, inp.Timetail_url, inp.Logname, inp.StartTime)
}

//{logname: nginx/access.log, timetail_port: 3132, timetail_url: '/timetail?log=',
func Get(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return body, nil
}
