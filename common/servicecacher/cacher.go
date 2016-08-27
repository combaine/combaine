package servicecacher

import (
	"sync"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

func NewService(n string, a ...interface{}) (Service, error) {
	return cocaine.NewService(n, a...)
}

type Service interface {
	Call(name string, args ...interface{}) chan cocaine.ServiceResult
	Close()
}

type ServiceBurner func(string, ...interface{}) (Service, error)

// cacher
type Cacher interface {
	Get(string, ...interface{}) (Service, error)
}

type cacher struct {
	mutex sync.Mutex
	fun   ServiceBurner
	cache map[string]*entry
}

func NewCacher(f ServiceBurner) Cacher {
	return &cacher{fun: f, cache: make(map[string]*entry)}
}

type entry struct {
	service Service
	err     error
	ready   chan struct{} // closed when res is ready
}

func (c *cacher) Get(name string, args ...interface{}) (Service, error) {
	var endpoint string
	if len(args) == 1 {
		endpoint, _ = args[0].(string)
	}
	key := name + endpoint

	c.mutex.Lock()
	e := c.cache[key]
	if e == nil || e.err != nil { // first request or service error
		e = &entry{ready: make(chan struct{})}
		c.cache[key] = e
		c.mutex.Unlock()
		e.service, e.err = c.fun(name, args...)
		close(e.ready) // broadcast ready condition
	} else {
		c.mutex.Unlock()
		<-e.ready // wait for ready condition
	}
	return e.service, e.err
}
