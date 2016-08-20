package cache

import (
	"testing"
)

func TestInMemory(t *testing.T) {
	data := []byte{100, 102}
	m, _ := NewInMemoryCache(nil)
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
