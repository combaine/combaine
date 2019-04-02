package combainer

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeysUtil(t *testing.T) {
	testMap := make(map[string]map[string]chan struct{})
	testMap["one"] = map[string]chan struct{}{
		"key1": make(chan struct{}),
		"key2": make(chan struct{}),
		"key3": make(chan struct{}),
	}

	mapKeys := keys(testMap["one"])
	sort.Sort(sort.StringSlice(mapKeys))
	assert.EqualValues(t, []string{"key1", "key2", "key3"}, mapKeys)

	testMap["two"] = map[string]chan struct{}{
		"key": make(chan struct{}),
	}
	assert.EqualValues(t, []string{"key"}, keys(testMap["two"]))
}
