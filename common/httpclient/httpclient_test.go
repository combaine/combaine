package httpclient

import (
	"testing"
)

func MainTest(t *testing.T) {
	const (
		CONNECTION_TIMEOUT = 1000
		RW_TIMEOUT         = 3000
	)
	c := NewClientWithTimeout(CONNECTION_TIMEOUT, RW_TIMEOUT)
	c.Get("http://ya.ru")
}
