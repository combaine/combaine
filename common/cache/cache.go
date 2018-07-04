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
	ttl          time.Duration
	interval     time.Duration
	cleanupAfter time.Duration
	store        map[string]*itemType
	runCleaner   sync.Once
}

// NewCache create new TTLCache instance
func NewCache(ttl time.Duration, interval time.Duration, cleanupAfter time.Duration) *TTLCache {
	c := &TTLCache{
		ttl:          ttl,
		interval:     interval,
		cleanupAfter: cleanupAfter,
		store:        make(map[string]*itemType),
	}
	go c.cleaner()
	return c
}

// TuneCache tune TTLCache ttl and interval
func (c *TTLCache) TuneCache(ttl time.Duration, interval time.Duration, cleanupAfter time.Duration) {
	c.Lock()
	c.ttl = ttl
	c.interval = interval
	c.cleanupAfter = cleanupAfter
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
		logger.Infof("%s Use cached entry for %s", id, key)
	}
	<-item.ready
	if time.Since(item.expires) > 0 {
		go func() {
			value, err := f()
			if err != nil {
				logger.Debugf("%s Failed to update stale cached entry for %s: %s", id, key, err)
				return
			}
			logger.Debugf("%s Update stale cached entry for %s", id, key)
			updated := &itemType{
				value:   value,
				err:     err,
				ready:   make(chan struct{}),
				expires: time.Now().Add(c.ttl),
			}
			c.Lock()
			c.store[key] = updated
			c.Unlock()
			close(updated.ready)
		}()
	}
	return item.value, item.err
}

// Delete element in the TTLCache
func (c *TTLCache) Delete(key string) {
	c.Lock()
	delete(c.store, key)
	c.Unlock()
}

func (c *TTLCache) cleaner() {
	logger.Debugf("Run TTLCache cleaner")
	var interval time.Duration
	for {
		c.RLock()
		interval = c.interval
		c.RUnlock()
		time.Sleep(interval)
		var staleItems []string
		c.RLock()
		for key, item := range c.store {
			if time.Since(item.expires) > c.cleanupAfter {
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
