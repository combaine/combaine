package servicecacher

import (
	"fmt"
	"testing"

	"github.com/combaine/combaine/tests"
	"github.com/stretchr/testify/assert"
)

func TestServicecacher(t *testing.T) {
	t.Log("Start")
	c := NewCacher(func(n string, a ...interface{}) (Service, error) {
		return tests.NewService(n, a...)
	})

	s1, err := c.Get("storage")
	assert.NoError(t, err)

	s2, err := c.Get("storage")
	assert.NoError(t, err)

	assert.Equal(t, s1, s2, "Unexpected copying")

	_, err = c.Get("errorService")
	assert.Error(t, err)

	for i := 0; i < 300; i++ {
		go func(i int) {
			_, err := c.Get(fmt.Sprintf("service_%d", i%10))
			assert.NoError(t, err)
		}(i)
	}

	t.Log("End")

	_, err = NewService("_NonExistingServicetestNewService_")
	assert.Error(t, err)
}
