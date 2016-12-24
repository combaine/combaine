package juggler

import (
	"sync"
	"time"
)

var (
	defaultCleanInterval = time.Minute * 5
	run                  sync.Once
)

// item holds cached item
type item struct {
	expires time.Time
	value   interface{}
}

// Cache is ttl cache for juggler api responses abount checks
type Cache struct {
	sync.RWMutex
	ttl      time.Duration
	interval time.Duration
	stop     chan struct{}
	store    map[string]*item
}

var globalCache = &Cache{
	ttl:   time.Minute,
	store: make(map[string]*item),
}

// RunCleaner run cleaner goroutine with cleanup interval
func (c *Cache) RunCleaner() {
	run.Do(func() {
		if c.interval == 0 {
			c.interval = defaultCleanInterval
		}
		go c.cleaner()
	})
}

// Get return not expired elevent from cacahe or nil
func (c *Cache) Get(key string) interface{} {
	c.RLock()
	item, present := c.store[key]
	c.RUnlock()
	if !present {
		return nil
	}
	if time.Now().Sub(item.expires) < 0 {
		return item.value
	}
	c.RLock()
	delete(c.store, key)
	c.RUnlock()
	return nil
}

// Set add new element in cache
func (c *Cache) Set(key string, value interface{}) {
	i := &item{
		value:   value,
		expires: time.Now().Add(c.ttl),
	}
	c.Lock()
	c.store[key] = i
	c.Unlock()
}

// Delete add new element in cache
func (c *Cache) Delete(key string) {
	c.Lock()
	delete(c.store, key)
	c.Unlock()
}

func (c *Cache) cleaner() {
	for {
		time.Sleep(c.interval)
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
