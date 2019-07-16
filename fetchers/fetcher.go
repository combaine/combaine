package fetchers

import (
	"context"
	"sync"

	"github.com/combaine/combaine/repository"
	"github.com/pkg/errors"
)

// FetcherTask task for hosts fetchers
type FetcherTask struct {
	ID     string
	Period int64
	Target string
}

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
	Fetch(ctx context.Context, task *FetcherTask) ([]byte, error)
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
