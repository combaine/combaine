package combainer

import (
	"testing"
)

func TestPool(t *testing.T) {
	p := NewPTaskMemoryPool()
	p1 := p.Get()
	p1.Host = "AAA"
	p.Put(p1)
	t.Log(p.Get())

	a := NewAggTaskMemoryPool()
	a1 := a.Get()
	a.Put(a1)
	t.Log(a.Get())
}
