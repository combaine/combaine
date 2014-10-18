package parsing

import (
	"fmt"
	"sync"

	"github.com/noxiouz/Combaine/common/tasks"
)

var fLock sync.Mutex
var fetchers = map[string]func(map[string]interface{}) (Fetcher, error){}

func Register(name string, f func(map[string]interface{}) (Fetcher, error)) {
	fLock.Lock()
	fetchers[name] = f
	fLock.Unlock()
}

type Fetcher interface {
	Fetch(task *tasks.FetcherTask) ([]byte, error)
}

func NewFetcher(name string, cfg map[string]interface{}) (f Fetcher, err error) {
	initializer, ok := fetchers[name]
	if !ok {
		err = fmt.Errorf("Fetcher %s isn't available", name)
		return
	}

	f, err = initializer(cfg)
	return
}
