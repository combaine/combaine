package httpfetcher

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
connection_timeout: 300
read_timeout: 20
port: 8089
uri: /TEST
`)

var http_cfg = map[string]interface{}{
	"connection_timeout": 300,
	"read_timeout":       10,
	"port":               8089,
}

func TestConfig(t *testing.T) {
	var cfg map[string]interface{}
	common.Decode(plain_config, &cfg)
	f, err := NewHttpFetcher(cfg)
	if err != nil {
		t.Fatal(err)
	}

	casted_f := f.(*HttpFetcher)
	assert.Equal(t, 300, casted_f.ConnTimeout)
	assert.Equal(t, 20, casted_f.ReadTimeout)
	assert.Equal(t, 8089, casted_f.Port)
	assert.Equal(t, "/TEST", casted_f.Uri)
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
	f, err := NewHttpFetcher(cfg)
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
		time.Sleep(3000 * time.Millisecond)
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
	cfg["read_timeout"] = 4000
	f, err := NewHttpFetcher(cfg)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Fetch(task)
	assert.NoError(t, err)
}
