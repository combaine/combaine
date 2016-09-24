package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrap(t *testing.T) {
	defer trap()
	panic(fmt.Errorf("test"))
	assert.Equal(t, true, true)
}
