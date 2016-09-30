package httpclient

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestClientPresent(t *testing.T) {
	assert.NotNil(t, client)
}

func TestMain(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/200":
			fmt.Fprintln(w, "200")
		case "/timeout":
			w.WriteHeader(200)
			time.Sleep(20 * time.Millisecond)
			fmt.Fprintln(w, "200")
		}
	}))
	defer ts.Close()

	ctx, closeF := context.WithTimeout(context.TODO(), 10*time.Millisecond)
	resp, err := Get(ctx, ts.URL+"/200")
	closeF()
	text, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, "200\n", string(text))
	assert.NoError(t, err)

	ctx, closeF = context.WithTimeout(context.TODO(), 10*time.Millisecond)
	req, _ := http.NewRequest("GET", ts.URL+"/200", nil)
	resp, err = Do(ctx, req)
	closeF()
	text, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, "200\n", string(text))
	assert.NoError(t, err)

	cl := NewClientWithTimeout(1, 1)
	_, err = cl.Get(ts.URL + "/timeout")
	assert.Contains(t, err.Error(), "i/o timeout")
	assert.Contains(t, err.Error(), "dial tcp")

	cl = NewClientWithTimeout(10*time.Millisecond, 10*time.Millisecond)
	_, err = cl.Get(ts.URL + "/timeout")
	assert.Contains(t, err.Error(), "i/o timeout")
	assert.Contains(t, err.Error(), "read tcp")
}
