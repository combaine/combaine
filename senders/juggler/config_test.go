package juggler

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/logger"
	"github.com/stretchr/testify/assert"
)

// GlobalCache is singleton for juggler sender

func TestStringifyLimits(t *testing.T) {
	cases := []map[string]interface{}{
		{"bytes": []byte("here"), "str": "s"},
		{"all": "str", "ing": "here"},
	}

	StringifyAggregatorLimits(cases)
	for _, i := range cases {
		for _, j := range i {
			assert.Equal(t, "string", fmt.Sprintf("%T", j))
		}
	}
}

func TestEnsureDefaultTags(t *testing.T) {
	cases := [][]string{
		{},
		{"combaine"},
		{"combaine", "someOtherTag"},
		{"someOtherTag"},
	}

	for _, tags := range cases {
		etags := EnsureDefaultTag(tags)
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
	GlobalCache = cache.NewCache(time.Minute /* ttl */, time.Minute*5 /* interval */, logger.LocalLogger())
	testConf := "testdata/config/juggler_example.yaml"
	os.Setenv("JUGGLER_CONFIG", testConf)
	sConf, err := GetSenderConfig()
	if err != nil {
		t.Fatal(err)
	}
	GlobalCache.TuneCache(sConf.CacheTTL, sConf.CacheCleanInterval)
	assert.Equal(t, sConf.CacheTTL, GlobalCache.GetTTL())
	assert.Equal(t, sConf.CacheCleanInterval, GlobalCache.GetInterval())
}
