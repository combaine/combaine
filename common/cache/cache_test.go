package cache

import (
	"testing"

	"github.com/combaine/combaine/common"
	"github.com/stretchr/testify/assert"
)

func TestInMemory(t *testing.T) {
	data := []byte{100, 102}
	_, err := NewCache(&common.PluginConfig{"type": "NotExist"})
	assert.Error(t, err)

	m, _ := NewCache(nil)
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("ReRegister cache not panic")
			}
		}()
		RegisterCache("InMemory", NewInMemoryCache)
	}()

	if err := m.Put("A", "A", data); err != nil {
		t.Fatalf("unable to put to InMemory cache: %s", err)
	}

	if d2, err := m.Get("A", "A"); err != nil {
		t.Fatalf("unable to get to InMemory cache: %s", err)
	} else if len(d2) != len(data) || d2[0] != data[0] {
		t.Fatalf("Corrupted data: %s %s", d2, data)
	}

	if _, err := m.Get("A", "B"); err == nil {
		t.Fatal("Got nil, but error was expected")
	}
}
