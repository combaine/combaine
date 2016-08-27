package tests

import (
	"sync"
	"sync/atomic"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/servicecacher"
)

var Results = make(chan []interface{})

type dummyResult []byte

func (dr dummyResult) Extract(i interface{}) error {
	t, _ := common.Pack(dr)
	return common.Unpack(t, i)
}
func (dr dummyResult) Err() error {
	return nil
}

type dummyService struct{}

func (ds *dummyService) Call(name string, args ...interface{}) chan cocaine.ServiceResult {
	Results <- args
	ch := make(chan cocaine.ServiceResult)
	go func() {
		ch <- dummyResult(args[1].([]byte))
	}()
	return ch
}
func (ds *dummyService) Close() { /* pass */ }
func NewService() servicecacher.Service {
	return &dummyService{}
}

type cache map[string]servicecacher.Service

type cacher struct {
	mutex sync.Mutex
	data  atomic.Value
}

func NewCacher() servicecacher.Cacher {
	c := &cacher{}
	c.data.Store(make(cache))

	return c
}

func (c *cacher) Get(name string, args ...interface{}) (s servicecacher.Service, err error) {
	s, ok := c.get(name)
	if ok {
		return
	}

	s, err = c.create(name)
	return
}

func (c *cacher) get(name string) (s servicecacher.Service, ok bool) {
	s, ok = c.data.Load().(cache)[name]
	return
}

func (c *cacher) create(name string) (servicecacher.Service, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	s, ok := c.get(name)
	if ok {
		return s, nil
	}

	s = NewService()
	data := c.data.Load().(cache)
	data[name] = s.(servicecacher.Service)
	c.data.Store(data)

	return s, nil
}
