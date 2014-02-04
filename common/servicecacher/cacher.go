package servicecacher

import (
	"sync"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

type Cacher interface {
	Get(name string) (*cocaine.Service, error)
}

type cacher struct {
	mutex sync.RWMutex
	data  map[string]*cocaine.Service
}

func NewCacher() (c Cacher) {
	return &cacher{
		data: make(map[string]*cocaine.Service),
	}
}

func (c *cacher) Get(name string) (s *cocaine.Service, err error) {
	s, ok := c.get(name)
	if ok {
		return
	}

	s, err = c.create(name)
	return
}

func (c *cacher) get(name string) (s *cocaine.Service, ok bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	s, ok = c.data[name]
	return
}

func (c *cacher) create(name string) (s *cocaine.Service, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	s, err = cocaine.NewService(name)
	if err != nil {
		return
	}
	c.data[name] = s
	return
}
