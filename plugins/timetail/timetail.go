package timetail

import (
	_ "fmt"
	"io/ioutil"
	"net/http"
)

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
