package servicecacher

import (
	"sync"
	"sync/atomic"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

type Cacher interface {
	Get(name string) (Service, error)
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

func (c *cacher) Get(name string) (s Service, err error) {
	s, ok := c.get(name)
	if ok {
		return
	}

	s, err = c.create(name)
	return
}

func (c *cacher) get(name string) (s Service, ok bool) {
	s, ok = c.data.Load().(cache)[name]
	return
}

func (c *cacher) create(name string) (Service, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	s, ok := c.get(name)
	if ok {
		return s, nil
	}

	s, err := cocaine.NewService(name)
	if err != nil {
		return nil, err
	}

	data := c.data.Load().(cache)
	data[name] = s
	c.data.Store(data)

	return s, nil
}
