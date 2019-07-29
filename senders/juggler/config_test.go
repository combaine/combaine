package juggler

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/combaine/combaine/common/cache"
	"github.com/stretchr/testify/assert"
)

func TestEnsureDefaultTags(t *testing.T) {
	cases := [][]string{
		{},
		{"combaine"},
		{"combaine", "someOtherTag"},
		{"someOtherTag"},
	}

	for _, tags := range cases {
		etags := ensureDefaultTag(tags)
		tagsSet := make(map[string]struct{}, len(etags))
		for _, tag := range etags {
			tagsSet[tag] = struct{}{}
		}
		for _, tag := range tags {
			_, ok := tagsSet[tag]
			assert.True(t, ok, fmt.Sprintf("Tag '%s' not present after ensureDefaultTag()", tag))
		}
		_, ok := tagsSet["combaine"]
		assert.True(t, ok, "Tag 'combaine' not present")
	}
}

func TestTuneCache(t *testing.T) {
	GlobalCache = cache.NewCache(time.Minute /* ttl */, time.Minute*5 /* interval */, time.Minute*5)
	testConf := "testdata/config/juggler_example.yaml"
	os.Setenv("JUGGLER_CONFIG", testConf)
	sConf, err := GetSenderConfig()
	if err != nil {
		t.Fatal(err)
	}
	GlobalCache.TuneCache(sConf.CacheTTL, sConf.CacheCleanInterval, sConf.CacheCleanInterval)
	assert.Equal(t, sConf.CacheTTL, GlobalCache.GetTTL())
	assert.Equal(t, sConf.CacheCleanInterval, GlobalCache.GetInterval())
}
