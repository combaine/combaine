package servicecacher

import (
	"testing"
)

func TestMain(t *testing.T) {
	t.Log("Start")
	c := NewCacher()
	s1, err := c.Get("storage")
	if err != nil {
		t.Fatalf("Unexpected error %s", err)
	}
	s2, err := c.Get("storage")
	if err != nil {
		t.Fatalf("Unexpected error %s", err)
	}
	if s1 != s2 {
		t.Fatalf("Unexpected copying %v, %v", s1, s2)
	}
	_, err = c.Get("magicservice")
	if err == nil {
		t.Fatalf("Unexpected error %s", err)
	}

}
