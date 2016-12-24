package juggler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCacheSetGetDelete(t *testing.T) {
	globalCache.ttl = time.Millisecond * 10
	key := "check_url"
	val := []byte("response")
	globalCache.Set(key, val)
	resp := globalCache.Get(key)
	assert.NotNil(t, resp)
	assert.Equal(t, val, resp.([]byte))
	globalCache.Delete(key)
	resp = globalCache.Get(key)
	assert.Nil(t, resp)

	// expiration item test
	globalCache.Set(key, val)
	time.Sleep(time.Millisecond * 15)
	assert.Nil(t, globalCache.Get(key))
	assert.Nil(t, globalCache.Get(key))
}

func TestCacheWithCleanupInterval(t *testing.T) {

	cache := &Cache{
		ttl:   time.Minute,
		store: make(map[string]*item),
	}

	cache.ttl = time.Millisecond * 10
	defaultCleanInterval = time.Millisecond * 20
	cache.RunCleaner()

	val := []byte("response")
	cache.Set("key1", val)
	cache.Set("key2", val)

	cases := []struct {
		sleep   time.Duration
		present bool
		message string
	}{
		{time.Millisecond * 15, true, "should be present"},
		{time.Millisecond * 15, true, "should be stil present"}, // item stale but present
		{time.Millisecond, false, "should be absent"},           // cleaner remove item
	}
	for _, c := range cases {
		cache.RLock()
		_, ok := cache.store["key1"]
		cache.RUnlock()
		assert.Equal(t, c.present, ok, c.message)
		time.Sleep(c.sleep)
	}

	cache.RLock()
	assert.Len(t, cache.store, 0)
	cache.RUnlock()
}
