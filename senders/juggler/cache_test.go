package juggler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCacheSetGetDelete(t *testing.T) {
	ctx := context.Background()
	checkFetcher := func(ctx context.Context, id, q string, hosts []string) ([]byte, error) {
		return []byte(hosts[0]), nil
	}

	id := "Test cache id"
	GlobalCache.TuneCache(time.Millisecond*5, time.Millisecond*10)
	key := "check_url"
	val := []string{"response"}
	resp, _ := GlobalCache.Get(ctx, key, checkFetcher, id, "q", val)
	assert.Len(t, resp, len(val[0]))
	assert.Equal(t, []byte(val[0]), resp)
	GlobalCache.Delete(key)
	resp, _ = GlobalCache.Get(ctx, key, checkFetcher, id, "q", val)
	assert.Len(t, resp, len(val[0]))
	GlobalCache.Delete(key)

	// expiration without cleaner test
	a, _ := GlobalCache.Get(ctx, key, checkFetcher, id, "q", []string{"One"})
	time.Sleep(time.Millisecond * 8)
	b, _ := GlobalCache.Get(ctx, key, checkFetcher, id, "q", []string{"Two"})
	assert.Equal(t, a, b)
}

func TestCacheWithCleanupInterval(t *testing.T) {

	myCache := &cache{
		ttl:      time.Minute * 10,
		interval: time.Minute * 20,
		store:    make(map[string]*itemType),
	}
	checkFetcher := func(ctx context.Context, id, q string, hosts []string) ([]byte, error) {
		return []byte(hosts[0]), nil
	}
	myCache.TuneCache(time.Millisecond*10, time.Millisecond*20)
	myCache.Get(context.TODO(), "key1", checkFetcher, "Test cache id", "q", []string{"val"})

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
	testConf := "testdata/config/juggler_example.yaml"
	os.Setenv("JUGGLER_CONFIG", testConf)
	sConf, err := GetSenderConfig()
	if err != nil {
		t.Fatal(err)
	}
	GlobalCache.TuneCache(sConf.CacheTTL, sConf.CacheCleanInterval)
	assert.Equal(t, sConf.CacheTTL, GlobalCache.ttl)
	assert.Equal(t, sConf.CacheCleanInterval, GlobalCache.interval)
}
