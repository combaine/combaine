package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrap(t *testing.T) {
	defer trap()
	assert.Equal(t, true, true)
	panic(fmt.Errorf("test"))
}
