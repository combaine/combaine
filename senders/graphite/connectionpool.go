package graphite

import (
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/combaine/combaine/common/logger"
)

var connPool Cacher

func init() {
	connPool = NewCacher(NewConn)
}

func NewConn(endpoint string, args ...interface{}) (conn io.WriteCloser, err error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("Not enought arguments")
	}
	retry, rok := args[0].(int)
	timeout, tok := args[1].(int)
	if !rok || !tok {
		return nil, fmt.Errorf("Failed to parse arguments retry or timeout")
	}

	for i := 0; i < retry; i++ {
		conn, err = net.DialTimeout("tcp", endpoint, time.Duration(timeout)*time.Millisecond)
		if err == nil {
			break
		}
		logger.Debugf("Failed to connect endpoint %s: %s", endpoint, err)
		time.Sleep(time.Duration(timeout))
	}
	if err != nil {
		return nil, fmt.Errorf("Unable to connect endpoin %s: %s after %d attempts", endpoint, err, retry)
	}
	return conn, err
}

type ServiceBurner func(string, ...interface{}) (io.WriteCloser, error)

// cacher
type Cacher interface {
	Get(string, ...interface{}) (io.WriteCloser, error)
	Evict(interface{})
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
	service io.WriteCloser
	err     error
	ready   chan struct{} // closed when res is ready
}

func (c *cacher) Get(name string, args ...interface{}) (io.WriteCloser, error) {
	c.mutex.Lock()
	e := c.cache[name]
	if e == nil { // first request or service error
		e = &entry{ready: make(chan struct{})}
		c.cache[name] = e
		c.mutex.Unlock()
		e.service, e.err = c.fun(name, args...)
		if e.err != nil {
			c.mutex.Lock()
			delete(c.cache, name)
			c.mutex.Unlock()
		}
		close(e.ready) // broadcast ready condition
	} else {
		c.mutex.Unlock()
		<-e.ready // wait for ready condition
	}
	return e.service, e.err
}

func (c *cacher) Evict(in interface{}) {
	addr := reflect.ValueOf(in).Pointer()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for n, v := range c.cache {
		if addr == reflect.ValueOf(v.service).Pointer() {
			delete(c.cache, n)
			v.service.Close()
			break
		}
	}
}
