package cache

import (
	"sync"
	"time"

	"github.com/combaine/combaine/common/logger"
)

// item holds cached item
type itemType struct {
	expires time.Time
	value   []byte
	err     error
	ready   chan struct{}
}

// TTLCache is ttl cache for http responses
type TTLCache struct {
	sync.RWMutex
	ttl        time.Duration
	interval   time.Duration
	store      map[string]*itemType
	runCleaner sync.Once
	logger     logger.Logger
}

// NewCache create new TTLCache instance
func NewCache(ttl time.Duration, interval time.Duration, log logger.Logger) *TTLCache {
	return &TTLCache{
		ttl:      ttl,
		interval: interval,
		store:    make(map[string]*itemType),
		logger:   log,
	}
}

// TuneCache tune TTLCache ttl and interval
func (c *TTLCache) TuneCache(ttl time.Duration, interval time.Duration) {
	c.Lock()
	c.ttl = ttl
	c.interval = interval
	c.Unlock()
}

type fetcher func() ([]byte, error)

// Get return not expired element from cacahe or nil
func (c *TTLCache) Get(id string, key string, f fetcher) ([]byte, error) {
	c.Lock()
	item := c.store[key]
	if item == nil {
		item = &itemType{
			ready:   make(chan struct{}),
			expires: time.Now().Add(c.ttl),
		}
		c.store[key] = item
		c.Unlock()
		item.value, item.err = f()
		if item.err != nil {
			c.Lock()
			delete(c.store, key)
			c.Unlock()
		}
		close(item.ready)
	} else {
		c.Unlock()
		<-item.ready
		c.logger.Infof("%s Use cached entry for %s", id, key)
	}
	c.runCleaner.Do(func() {
		c.logger.Debug("run TTLCache cleaner")
		go c.cleaner()
	})
	if time.Now().Sub(item.expires) >= 0 {
		c.logger.Debugf("%s remove stale cached entry for %s", id, key)
		c.Lock()
		delete(c.store, key)
		c.Unlock()
	}
	return item.value, item.err
}

// Delete add new element in the TTLCache
func (c *TTLCache) Delete(key string) {
	c.Lock()
	delete(c.store, key)
	c.Unlock()
}

func (c *TTLCache) cleaner() {
	var interval time.Duration
	for {
		c.RLock()
		interval = c.interval
		c.RUnlock()
		time.Sleep(interval)
		var staleItems []string
		c.RLock()
		for key, item := range c.store {
			if time.Now().Sub(item.expires) > 0 {
				staleItems = append(staleItems, key)
			}
		}
		c.RUnlock()
		if len(staleItems) > 0 {
			c.Lock()
			for _, k := range staleItems {
				delete(c.store, k)
			}
			c.Unlock()
		}
	}
}

// GetTTL of internal store
func (c *TTLCache) GetTTL() time.Duration {
	return c.ttl
}

// GetInterval cleaner of internal store
func (c *TTLCache) GetInterval() time.Duration {
	return c.interval
}
