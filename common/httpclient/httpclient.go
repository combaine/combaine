package httpclient

import (
	"io"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
)

var client *http.Client

func init() {
	client = NewClient()
}

func newDialer(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		d := net.Dialer{DualStack: true}
		if cTimeout != 0 {
			d.Timeout = cTimeout
		}
		conn, err := d.Dial(netw, addr)
		if err != nil {
			return nil, err
		}
		if rwTimeout != 0 {
			conn.SetDeadline(time.Now().Add(rwTimeout))
		}
		return conn, nil
	}
}

// NewClientWithTimeout create HTTP Client, which has connection and r/w timeouts
func NewClientWithTimeout(connectTimeout time.Duration, rwTimeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{Dial: newDialer(connectTimeout, rwTimeout)},
	}
}

// NewClient create HTTP Client with DualStack dialler
func NewClient() *http.Client {
	return &http.Client{Transport: &http.Transport{Dial: newDialer(0, 0)}}
}

// Do perform ctxhttp.Do request with predefined client with DualStack dialler
func Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	return ctxhttp.Do(ctx, client, req)
}

// Get perform ctxhttp.Get request with predefined client with DualStack dialler
func Get(ctx context.Context, url string) (*http.Response, error) {
	return ctxhttp.Get(ctx, client, url)
}

// Post perform ctxhttp.Post request with predefined client with DualStack dialler
func Post(ctx context.Context, url, contentType string, body io.Reader) (*http.Response, error) {
	return ctxhttp.Post(ctx, client, url, contentType, body)
}
