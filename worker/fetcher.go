package worker

import (
	"context"
	"sync"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/pkg/errors"
)

var fLock sync.Mutex
var fetchers = map[string]func(repository.PluginConfig) (Fetcher, error){}

// Register new fetcher initializer
func Register(name string, f func(repository.PluginConfig) (Fetcher, error)) {
	fLock.Lock()
	fetchers[name] = f
	fLock.Unlock()
}

// Fetcher interface
type Fetcher interface {
	Fetch(ctx context.Context, task *common.FetcherTask) ([]byte, error)
}

// NewFetcher get and initialize new fetcher
func NewFetcher(name string, cfg repository.PluginConfig) (f Fetcher, err error) {
	initializer, ok := fetchers[name]
	if !ok {
		err = errors.New("Fetcher " + name + " isn't available")
		return
	}

	f, err = initializer(cfg)
	return
}
