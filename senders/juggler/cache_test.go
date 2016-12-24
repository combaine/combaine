package juggler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCacheSetGetDelete(t *testing.T) {
	GlobalCache.ttl = time.Millisecond * 10
	key := "check_url"
	val := []byte("response")
	GlobalCache.Set(key, val)
	resp := GlobalCache.Get(key)
	assert.NotNil(t, resp)
	assert.Equal(t, val, resp.([]byte))
	GlobalCache.Delete(key)
	resp = GlobalCache.Get(key)
	assert.Nil(t, resp)

	// expiration item test
	GlobalCache.Set(key, val)
	time.Sleep(time.Millisecond * 15)
	assert.Nil(t, GlobalCache.Get(key))
	assert.Nil(t, GlobalCache.Get(key))
}

func TestCacheWithCleanupInterval(t *testing.T) {

	myCache := &cache{
		ttl:      time.Minute * 10,
		interval: time.Minute * 20,
		store:    make(map[string]*item),
	}
	myCache.TuneCache(time.Millisecond*10, time.Millisecond*20)

	val := []byte("response")
	go myCache.Set("key1", val)
	myCache.Set("key1", val)
	myCache.Set("key2", val)

	cases := []struct {
		sleep   time.Duration
		present bool
		message string
	}{
		{time.Millisecond * 15, true, "should be present"},
		{time.Millisecond * 15, true, "should be stil present"}, // item stale but present
		{time.Millisecond * 1, false, "should be absent"},       // cleaner remove item
	}
	for _, c := range cases {
		myCache.RLock()
		_, ok := myCache.store["key1"]
		myCache.RUnlock()
		assert.Equal(t, c.present, ok, c.message)
		time.Sleep(c.sleep)
	}
	time.Sleep(time.Millisecond * 15)

	myCache.RLock()
	assert.Len(t, myCache.store, 0)
	myCache.RUnlock()
}

func TestTuneCache(t *testing.T) {
	sConf, _ := GetSenderConfig()
	GlobalCache.TuneCache(sConf.CacheTTL, sConf.CacheCleanInterval)
	assert.Equal(t, sConf.CacheTTL, GlobalCache.ttl)
	assert.Equal(t, sConf.CacheCleanInterval, GlobalCache.interval)
}
