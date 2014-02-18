package parsing

import (
	"sync"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

var dgMutex sync.RWMutex

var dgrids = map[string]Datagrid{}

type Datagrid interface {
	Put(data interface{}) (token string, err error)
	Drop(token string) error
}

type dg struct {
	s *cocaine.Service
}

func (d *dg) Put(data interface{}) (token string, err error) {
	return
}

func (d *dg) Drop(token string) (err error) {
	return
}

func GetDG(name string) (d Datagrid, err error) {
	dgMutex.RLock()
	d, ok := dgrids[name]
	if ok {
		dgMutex.RUnlock()
		return
	}

	// Create
	dgMutex.Lock()
	defer dgMutex.Unlock()
	app, err := cocaine.NewService(name)
	if err != nil {
		return
	}

	d = &dg{s: app}
	return
}
