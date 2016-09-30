package httpclient

import (
	"context"
	"net"
	"net/http"
	"time"

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

// HTTP Client, which has connection and r/w timeouts
func NewClientWithTimeout(connectTimeout time.Duration, rwTimeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{Dial: newDialer(connectTimeout, rwTimeout)},
	}
}

func NewClient() *http.Client {
	return &http.Client{Transport: &http.Transport{Dial: newDialer(0, 0)}}
}

func Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	return ctxhttp.Do(ctx, client, req)
}

func Get(ctx context.Context, url string) (*http.Response, error) {
	return ctxhttp.Get(ctx, client, url)
}
