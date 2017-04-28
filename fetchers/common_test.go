package fetchers

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/combaine/combaine/common"

	"github.com/stretchr/testify/assert"
)

var (
	withErr = true
	ok      = false
)

func TestTCPSocketFetcherConfig(t *testing.T) {
	var plainConfig = []byte("connection_timeout: 300\nport: 18089")
	var cfg common.PluginConfig
	common.Decode(plainConfig, &cfg)
	f, err := NewTCPSocketFetcher(cfg)
	assert.NoError(t, err)

	_, err = NewTCPSocketFetcher(common.PluginConfig{})
	assert.Error(t, err)

	castedF := f.(*tcpSocketFetcher)
	assert.Equal(t, 300, castedF.Timeout)
	assert.Equal(t, "18089", castedF.Port)
}

func TestTCPSocketFetcherFetch(t *testing.T) {
	l, err := net.Listen("tcp4", "")
	assert.NoError(t, err)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			conn.Write([]byte("hello"))
			conn.Close()
		}
	}()
	defer l.Close()
	target, port, _ := net.SplitHostPort(l.Addr().String())

	cases := []struct {
		err      bool
		expected string
		config   common.PluginConfig
	}{
		{ok, "hello", common.PluginConfig{"connection_timeout": 150}},
	}

	for _, c := range cases {
		task := &common.FetcherTask{Task: common.Task{Id: "ID"}, Target: target}

		c.config["port"], _ = strconv.Atoi(port)
		f, err := NewTCPSocketFetcher(c.config)
		assert.NoError(t, err)

		if body, err := f.Fetch(task); c.err {
			assert.EqualValues(t, context.DeadlineExceeded, err)
		} else {
			assert.EqualValues(t, c.expected, body)
			assert.NoError(t, err)
		}
	}
}

func TestHTTPFetcherConfig(t *testing.T) {
	var plainConfig = []byte(`
connection_timeout: 300
read_timeout: 300
port: 8089
uri: /TEST`)

	var cfg common.PluginConfig
	common.Decode(plainConfig, &cfg)
	f, err := NewHTTPFetcher(cfg)
	assert.NoError(t, err)

	_, err = NewHTTPFetcher(common.PluginConfig{})
	assert.Error(t, err)

	castedF := f.(*httpFetcher)
	assert.Equal(t, 300, castedF.Timeout)
	assert.Equal(t, 8089, castedF.Port)
	assert.Equal(t, "/TEST", castedF.URI)
}

func TestHTTPFetcherFetch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log(time.Now())
		time.Sleep(100 * time.Millisecond)
		t.Log(time.Now())
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()
	target, port, _ := net.SplitHostPort(ts.Listener.Addr().String())

	cases := []struct {
		err    bool
		config common.PluginConfig
	}{
		{true, common.PluginConfig{"read_timeout": 50}},
		{false, common.PluginConfig{"read_timeout": 150}},
	}

	for _, c := range cases {
		task := &common.FetcherTask{Task: common.Task{Id: "ID"}, Target: target}

		c.config["port"], _ = strconv.Atoi(port)
		f, err := NewHTTPFetcher(c.config)
		assert.NoError(t, err)

		if _, err = f.Fetch(task); c.err {
			assert.EqualValues(t, context.DeadlineExceeded, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestTimetailFetcherConfig(t *testing.T) {

	var plainConfig = []byte(`
connection_timeout: 30000
read_timeout: 300000
timetail_port: 3132
timetail_url: '/timetail?pattern=request_time&log_ts='
`)

	var cfg common.PluginConfig
	common.Decode(plainConfig, &cfg)
	f, err := NewTimetailFetcher(cfg)
	assert.NoError(t, err)

	_, err = NewTimetailFetcher(common.PluginConfig{})
	assert.Error(t, err)

	castedF := f.(*timetailFetcher)
	assert.Equal(t, 300000, castedF.Timeout)
	assert.Equal(t, 3132, castedF.Port)
	assert.Equal(t, "/timetail?pattern=request_time&log_ts=", castedF.URL)
}

func TestTimetailFetcherFetch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log(time.Now())
		time.Sleep(100 * time.Millisecond)
		t.Log(time.Now())
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()
	target, port, _ := net.SplitHostPort(ts.Listener.Addr().String())

	cases := []struct {
		err    bool
		config common.PluginConfig
	}{
		{withErr, common.PluginConfig{"read_timeout": 10, "timetail_url": "/TEST"}},
		{ok, common.PluginConfig{"read_timeout": 110, "timetail_url": "/TEST"}},
	}

	for _, c := range cases {
		task := &common.FetcherTask{Task: common.Task{Id: "ID"}, Target: target}

		c.config["timetail_port"], _ = strconv.Atoi(port)
		f, err := NewTimetailFetcher(c.config)
		assert.NoError(t, err)

		if _, err = f.Fetch(task); c.err {
			assert.EqualValues(t, context.DeadlineExceeded, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
