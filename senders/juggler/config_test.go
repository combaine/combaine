package juggler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringifyLimits(t *testing.T) {
	cases := []map[string]interface{}{
		map[string]interface{}{"bytes": []byte("here"), "str": "s"},
		map[string]interface{}{"all": "str", "ing": "here"},
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
		[]string{},
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
