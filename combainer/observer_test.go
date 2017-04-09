package combainer

import (
	"testing"

	"github.com/combaine/combaine/common/cache"
	"github.com/stretchr/testify/assert"
)

func TestRegisterClient(t *testing.T) {
	c1, _ := NewClient(&cache.InMemory{}, repo)

	GlobalObserver.RegisterClient(c1, "singleConfig")
	c1.AddSuccessAggregate()
	c1.AddFailedAggregate()
	stats := GlobalObserver.GetClientsStats()
	assert.EqualValues(t, c1.failedAggregate, 1)
	assert.EqualValues(t, c1.successAggregate, 1)
	assert.EqualValues(t, stats["singleConfig"].AggregateTotal, 2)

	c2, _ := NewClient(&cache.InMemory{}, repo)
	GlobalObserver.RegisterClient(c2, "singleConfig") // ReRegister client for config c1
	stats = GlobalObserver.GetClientsStats()
	assert.EqualValues(t, c2.failedAggregate, 1)
	assert.EqualValues(t, c2.successAggregate, 1)
	assert.EqualValues(t, stats["singleConfig"].AggregateTotal, 2)

	GlobalObserver.UnregisterClient(c1.ID, "singleConfig")
	stats = GlobalObserver.GetClientsStats()
	assert.True(t, len(stats) == 1)
	assert.True(t, stats["singleConfig"] != nil)

	GlobalObserver.UnregisterClient(c2.ID, "singleConfig")
	stats = GlobalObserver.GetClientsStats()
	assert.True(t, len(stats) == 0)
	assert.True(t, stats["singleConfig"] == nil)
}
