package httpclient

import (
	"testing"
)

func MainTest(t *testing.T) {
	const (
		CONNECTION_TIMEOUT = 1000
		RW_TIMEOUT         = 3000
	)
	_ := NewClientWithTimeout(CONNECTION_TIMEOUT, RW_TIMEOUT)
}
