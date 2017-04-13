package worker

import (
	"fmt"
	"sync"

	"github.com/combaine/combaine/common"
)

var fLock sync.Mutex
var fetchers = map[string]func(common.PluginConfig) (Fetcher, error){}

func Register(name string, f func(common.PluginConfig) (Fetcher, error)) {
	fLock.Lock()
	fetchers[name] = f
	fLock.Unlock()
}

type Fetcher interface {
	Fetch(task *common.FetcherTask) ([]byte, error)
}

func NewFetcher(name string, cfg common.PluginConfig) (f Fetcher, err error) {
	initializer, ok := fetchers[name]
	if !ok {
		err = fmt.Errorf("Fetcher %s isn't available", name)
		return
	}

	f, err = initializer(cfg)
	return
}
