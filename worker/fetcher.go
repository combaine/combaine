package worker

import (
	"sync"

	"github.com/combaine/combaine/common"
	"github.com/pkg/errors"
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
		err = errors.New("Fetcher " + name + " isn't available")
		return
	}

	f, err = initializer(cfg)
	return
}
