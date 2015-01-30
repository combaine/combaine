package httpclient

import (
	"net"
	"net/http"
	"time"
)

func timeoutDialer(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		d := net.Dialer{
			Timeout:   cTimeout,
			DualStack: true,
		}
		conn, err := d.Dial(netw, addr)
		if err != nil {
			return nil, err
		}
		conn.SetDeadline(time.Now().Add(rwTimeout))
		return conn, nil
	}
}

// HTTP Client, which has connection and r/w timeouts
func NewClientWithTimeout(connectTimeout time.Duration, rwTimeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Dial: timeoutDialer(connectTimeout, rwTimeout),
		},
	}
}
