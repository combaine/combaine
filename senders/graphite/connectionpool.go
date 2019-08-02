package graphite

import (
	"context"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var connPool Cacher

func init() {
	connPool = NewCacher(NewConn)
}

// NewConn provide new connections for connection pool
func NewConn(endpoint string, args ...interface{}) (conn io.WriteCloser, err error) {
	if len(args) < 2 {
		return nil, errors.New("Not enought arguments")
	}
	retry, rok := args[0].(int)
	timeout, tok := args[1].(int)
	if !rok || !tok {
		return nil, errors.New("Failed to parse arguments retry or timeout")
	}

	for i := 1; i <= retry; i++ {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(timeout)*time.Millisecond)
		dialer := net.Dialer{DualStack: true}
		conn, err = dialer.DialContext(ctx, "tcp", endpoint)
		cancel()
		if err == nil {
			break
		} else {
			err = errors.Errorf("Unable to connect endpoin %s: %s after %d attempts", endpoint, err, i)
		}
		logrus.Debugf("Failed to connect endpoint %s: %s", endpoint, err)
		if i < retry {
			time.Sleep(time.Duration(reconnectInterval) * time.Millisecond)
		}
	}
	return
}

// ServiceBurner wrapper for hide cocaine.NewService
type ServiceBurner func(string, ...interface{}) (io.WriteCloser, error)

// Cacher interface
type Cacher interface {
	Get(string, ...interface{}) (io.WriteCloser, error)
	Evict(interface{})
}

type cacher struct {
	mutex sync.Mutex
	fun   ServiceBurner
	cache map[string]*entry
}

// NewCacher take function that provide connections
// for connection pool and reurn connection pool
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
