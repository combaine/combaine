package juggler

import (
	"sync"
	"time"
)

// item holds cached item
type item struct {
	expires time.Time
	value   interface{}
}

// cache is ttl cache for juggler api responses about checks
type cache struct {
	sync.RWMutex
	ttl            time.Duration
	interval       time.Duration
	store          map[string]*item
	cleanerPresent bool
}

// GlobalCache is singleton for juggler sender
var GlobalCache = &cache{
	ttl:      time.Minute,
	interval: time.Minute * 5,
	store:    make(map[string]*item),
}

// TuneCache tune cache ttl and interval
func (c *cache) TuneCache(ttl time.Duration, interval time.Duration) {
	c.Lock()
	c.ttl = ttl
	c.interval = interval
	c.Unlock()
}

// Get return not expired elevent from cacahe or nil
func (c *cache) Get(key string) interface{} {
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
func (c *cache) Set(key string, value interface{}) {
	c.RLock()
	ttl := c.ttl
	c.RUnlock()

	i := &item{
		value:   value,
		expires: time.Now().Add(ttl),
	}
	c.Lock()
	c.store[key] = i
	if !c.cleanerPresent {
		c.cleanerPresent = true
		go c.cleaner()
	}
	c.Unlock()

}

// Delete add new element in cache
func (c *cache) Delete(key string) {
	c.Lock()
	delete(c.store, key)
	c.Unlock()
}

func (c *cache) cleaner() {
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
