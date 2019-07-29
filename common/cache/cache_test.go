package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var GlobalCache = NewCache(time.Minute*10 /* ttl*/, time.Minute*20 /* interval*/, time.Minute*20)

func TestCacheSetGetDelete(t *testing.T) {
	val := []string{"response"}
	checkFetcher := func() ([]string, error) {
		return val, nil
	}

	id := "TestCacheSetGetDelete"
	GlobalCache.TuneCache(time.Millisecond*5, time.Millisecond*10, time.Microsecond*10)
	key := "check_url"
	resp, _ := GlobalCache.GetStrings(id, key, checkFetcher)
	assert.Len(t, resp, len(val))
	assert.Equal(t, val, resp)
	GlobalCache.Delete(key)
	resp, _ = GlobalCache.GetStrings(id, key, checkFetcher)
	assert.Len(t, resp, len(val))
	GlobalCache.Delete(key)

	// expiration without cleaner test
	expected := []byte("Result")
	var bytesFetcher = func() ([]byte, error) { return expected, nil }

	a, _ := GlobalCache.GetBytes(id, key, bytesFetcher)
	assert.Equal(t, expected, a)
	time.Sleep(time.Millisecond * 5)
	bytesFetcher = func() ([]byte, error) { return []byte("Updated"), nil }
	b, _ := GlobalCache.GetBytes(id, key, bytesFetcher)
	assert.Equal(t, expected, b)
}

func TestCacheWithCleanupInterval(t *testing.T) {

	myCache := NewCache(
		time.Minute*10, // ttl
		time.Minute*20, // interval
		time.Minute*20, // cleanupAfter
	)
	mapFetcher := func() (map[string][]string, error) { return map[string][]string{"expected": {"map"}}, nil }
	myCache.TuneCache(time.Millisecond*10, time.Millisecond*20, time.Microsecond*20)
	myCache.GetMapStringStrings("TestCacheWithCleanupInterval", "key1", mapFetcher)

	cases := []struct {
		present bool
		message string
		sleep   time.Duration
	}{
		{true, "value should be present", time.Millisecond * 10},
		{true, "value should be still present", time.Millisecond * 30}, // item stale but present
		{false, "value should be absent", time.Millisecond * 1},        // cleaner remove item
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
