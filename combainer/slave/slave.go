package slave

import (
	"errors"
	"golang.org/x/net/context"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

var ErrTimeout = errors.New("operation timed out")

type AsyncResult interface {
	Wait(ctx context.Context, result interface{}) error
}

type Slave interface {
	Close()
	Do(name string, event string, payload []byte) AsyncResult
}

type asyncResult struct {
	ch chan cocaine.ServiceResult
}

func (a *asyncResult) Wait(ctx context.Context, result interface{}) error {
	select {
	case res := <-a.ch:
		if res.Err() != nil {
			return res.Err()
		}
		return res.Extract(result)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type slave struct {
	*cocaine.Service
}

func NewSlave(app *cocaine.Service) Slave {
	return &slave{
		Service: app,
	}
}

func (s *slave) Close() {
	s.Service.Close()
}

func (s *slave) Do(name string, event string, payload []byte) AsyncResult {
	return &asyncResult{
		ch: s.Service.Call(name, event, payload),
	}
}
