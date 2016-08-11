package timetail

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/Combaine/Combaine/common"
	"github.com/Combaine/Combaine/common/tasks"

	"github.com/stretchr/testify/assert"
)

var plain_config = []byte(`
connection_timeout: 30000
read_timeout: 300000
timetail_port: 3132
timetail_url: '/timetail?pattern=request_time&log_ts='
`)

var http_cfg = map[string]interface{}{
	"connection_timeout": 300,
	"read_timeout":       10,
	"timetail_port":      8089,
	"timetail_url":       "/TEST",
}

func TestConfig(t *testing.T) {
	var cfg map[string]interface{}
	common.Decode(plain_config, &cfg)
	f, err := NewTimetail(cfg)
	if err != nil {
		t.Fatal(err)
	}

	casted_f := f.(*Timetail)
	assert.Equal(t, 30000, casted_f.TimetailConfig.ConnTimeout)
	assert.Equal(t, 300000, casted_f.TimetailConfig.ReadTimeout)
	assert.Equal(t, 3132, casted_f.TimetailConfig.Port)
	assert.Equal(t, "/timetail?pattern=request_time&log_ts=", casted_f.TimetailConfig.Url)
}

func TestTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("%v\n", time.Now())
		time.Sleep(600 * time.Millisecond)
		fmt.Printf("%v\n", time.Now())
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()

	target, port, _ := net.SplitHostPort(ts.Listener.Addr().String())

	task := &tasks.FetcherTask{
		CommonTask: tasks.CommonTask{Id: "ID"},
		Target:     target,
	}

	cfg := http_cfg
	cfg["port"], _ = strconv.Atoi(port)
	f, err := NewTimetail(cfg)
	if err != nil {
		t.Fatal(err)
	}

	st := time.Now()
	_, err = f.Fetch(task)
	assert.Error(t, err)
	assert.True(t, time.Now().Sub(st) < 20*time.Millisecond)
}

func TestNonTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("%v\n", time.Now())
		time.Sleep(3300 * time.Millisecond)
		fmt.Printf("%v\n", time.Now())
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()

	target, port, _ := net.SplitHostPort(ts.Listener.Addr().String())

	task := &tasks.FetcherTask{
		CommonTask: tasks.CommonTask{Id: "ID"},
		Target:     target,
	}

	cfg := http_cfg
	cfg["timetail_port"], _ = strconv.Atoi(port)
	cfg["read_timeout"] = 4000
	f, err := NewTimetail(cfg)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Fetch(task)
	assert.NoError(t, err)
}
