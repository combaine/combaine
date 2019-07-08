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

	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/worker"

	"github.com/stretchr/testify/assert"
)

var (
	withErr = true
	ok      = false
)

func TestTCPSocketFetcherConfig(t *testing.T) {
	var plainConfig = repository.EncodedConfig("port: 18089")
	var cfg repository.PluginConfig
	plainConfig.Decode(&cfg)
	f, err := NewTCPSocketFetcher(cfg)
	assert.NoError(t, err)

	_, err = NewTCPSocketFetcher(repository.PluginConfig{})
	assert.Error(t, err)

	castedF := f.(*tcpSocketFetcher)
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
		config   repository.PluginConfig
	}{
		{ok, "hello", repository.PluginConfig{}},
	}

	for _, c := range cases {
		task := &worker.FetcherTask{ID: "ID", Target: target}

		c.config["port"], _ = strconv.Atoi(port)
		f, err := NewTCPSocketFetcher(c.config)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if body, err := f.Fetch(ctx, task); c.err {
			assert.EqualValues(t, context.DeadlineExceeded, err)
		} else {
			assert.EqualValues(t, c.expected, body)
			assert.NoError(t, err)
		}
	}
}

func TestHTTPFetcherConfig(t *testing.T) {
	var plainConfig = repository.EncodedConfig("port: 8089\nuri: /TEST\n")

	var cfg repository.PluginConfig
	plainConfig.Decode(&cfg)
	f, err := NewHTTPFetcher(cfg)
	assert.NoError(t, err)

	_, err = NewHTTPFetcher(repository.PluginConfig{})
	assert.Error(t, err)

	castedF := f.(*httpFetcher)
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
		err     bool
		config  repository.PluginConfig
		timeout time.Duration
	}{
		{true, repository.PluginConfig{}, 50 * time.Millisecond},
		{false, repository.PluginConfig{}, 150 * time.Millisecond},
	}

	for _, c := range cases {
		task := &worker.FetcherTask{ID: "ID", Target: target}

		c.config["port"], _ = strconv.Atoi(port)
		f, err := NewHTTPFetcher(c.config)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		if _, err = f.Fetch(ctx, task); c.err {
			assert.EqualValues(t, context.DeadlineExceeded, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestTimetailFetcherConfig(t *testing.T) {

	var plainConfig = repository.EncodedConfig("timetail_port: 3132\ntimetail_url: '/timetail?pattern=request_time&log_ts='\n")

	var cfg repository.PluginConfig
	plainConfig.Decode(&cfg)
	f, err := NewTimetailFetcher(cfg)
	assert.NoError(t, err)

	_, err = NewTimetailFetcher(repository.PluginConfig{})
	assert.Error(t, err)

	castedF := f.(*timetailFetcher)
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
		err     bool
		config  repository.PluginConfig
		timeout time.Duration
	}{
		{withErr, repository.PluginConfig{"timetail_url": "/TEST"}, 10 * time.Millisecond},
		{ok, repository.PluginConfig{"timetail_url": "/TEST"}, 200 * time.Millisecond},
	}

	for _, c := range cases {
		task := &worker.FetcherTask{ID: "ID", Target: target}

		c.config["timetail_port"], _ = strconv.Atoi(port)
		f, err := NewTimetailFetcher(c.config)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		if _, err = f.Fetch(ctx, task); c.err {
			assert.EqualValues(t, context.DeadlineExceeded, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
