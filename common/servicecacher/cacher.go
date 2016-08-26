package servicecacher

import (
	"sync"
	"sync/atomic"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

type Cacher interface {
	Get(name string, args ...interface{}) (Service, error)
}

type Service interface {
	Call(name string, args ...interface{}) chan cocaine.ServiceResult
	Close()
}

type cache map[string]Service

type cacher struct {
	mutex sync.Mutex
	data  atomic.Value
}

func NewCacher() Cacher {
	c := &cacher{}
	c.data.Store(make(cache))

	return c
}

func (c *cacher) Get(name string, args ...interface{}) (s Service, err error) {
	var endpoint string
	if len(args) == 1 {
		endpoint, _ = args[0].(string)
	}

	s, ok := c.get(name + endpoint)
	if ok {
		return
	}

	s, err = c.create(name, args...)
	return
}

func (c *cacher) get(name string) (s Service, ok bool) {
	s, ok = c.data.Load().(cache)[name]
	return
}

func (c *cacher) create(name string, args ...interface{}) (Service, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var endpoint string
	if len(args) == 1 {
		endpoint, _ = args[0].(string)
	}
	s, ok := c.get(name)
	if ok {
		return s, nil
	}
	s, err := cocaine.NewService(name, args...)
	if err != nil {
		return nil, err
	}
	data := c.data.Load().(cache)
	data[name+endpoint] = s
	c.data.Store(data)

	return s, nil
}
