package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var GlobalCache = NewCache(time.Minute*10 /* ttl*/, time.Minute*20 /* interval*/, time.Minute*20)

func TestCacheSetGetDelete(t *testing.T) {
	val := []string{"response"}
	checkFetcher := func() (interface{}, error) {
		return []byte(val[0]), nil
	}

	id := "TestCacheSetGetDelete"
	GlobalCache.TuneCache(time.Millisecond*5, time.Millisecond*10, time.Microsecond*10)
	key := "check_url"
	resp, _ := GlobalCache.Get(id, key, checkFetcher)
	assert.Len(t, resp, len(val[0]))
	assert.Equal(t, []byte(val[0]), resp)
	GlobalCache.Delete(key)
	resp, _ = GlobalCache.Get(id, key, checkFetcher)
	assert.Len(t, resp, len(val[0]))
	GlobalCache.Delete(key)

	// expiration without cleaner test
	expected := []byte("Result")
	checkFetcher = func() (interface{}, error) { return expected, nil }

	a, _ := GlobalCache.Get(id, key, checkFetcher)
	assert.Equal(t, expected, a)
	time.Sleep(time.Millisecond * 5)
	checkFetcher = func() (interface{}, error) { return []byte("Updated"), nil }
	b, _ := GlobalCache.Get(id, key, checkFetcher)
	assert.Equal(t, expected, b)
}

func TestCacheWithCleanupInterval(t *testing.T) {

	myCache := NewCache(
		time.Minute*10, // ttl
		time.Minute*20, // interval
		time.Minute*20, // cleanupAfter
	)
	checkFetcher := func() (interface{}, error) { return []byte("expected"), nil }
	myCache.TuneCache(time.Millisecond*10, time.Millisecond*20, time.Microsecond*20)
	myCache.Get("TestCacheWithCleanupInterval", "key1", checkFetcher)

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
