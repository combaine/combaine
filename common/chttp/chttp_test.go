package chttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var ts *httptest.Server

func TestMain(m *testing.M) {
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get-sleep":
			time.Sleep(20)
			w.Write([]byte("ok"))
		case "/get":
			w.Write([]byte("ok"))
		case "/post":
			reqBytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(500)
				fmt.Fprintln(w, err)
			}
			w.WriteHeader(200)
			io.Copy(w, bytes.NewReader(reqBytes))
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Not Found")
		}
	}))
	defer ts.Close()
	os.Exit(m.Run())
}
func TestUserAgent(t *testing.T) {
	var baseURL = "http://" + ts.Listener.Addr().String()
	var customUA = "customUA"
	req, _ := http.NewRequest("GET", baseURL, nil)
	req.Header.Set("User-Agent", customUA)
	Do(context.Background(), req)
	assert.Equal(t, req.Header.Get("User-Agent"), customUA)

	req, _ = http.NewRequest("GET", baseURL, nil)
	Do(context.Background(), req)
	assert.Equal(t, req.Header.Get("User-Agent"), defaultUserAgent)
}

func TestGet(t *testing.T) {
	var baseURL = "http://" + ts.Listener.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := Get(ctx, "%%#bad-url")
	assert.Error(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 10)
	_, err = Get(ctx, baseURL+"/get-sleep")
	assert.Equal(t, err, context.DeadlineExceeded)
	cancel()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	resp, err := Get(ctx, baseURL+"/get")
	assert.Nil(t, err)
	text, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, text, []byte("ok"))
	cancel()

}

func TestPost(t *testing.T) {
	var baseURL = "http://" + ts.Listener.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := Post(ctx, "%%#bad-url", "", nil)
	assert.Error(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	resp, err := Post(ctx, baseURL+"/post", "application/json", bytes.NewReader([]byte("PostData")))
	assert.Nil(t, err)
	text, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, text, []byte("PostData"))
	cancel()
}
