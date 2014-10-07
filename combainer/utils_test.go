package combainer

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"testing"
)

func TestMain(t *testing.T) {
	const (
		key   = "KEY"
		value = "VALUE"
	)

	cache, err := NewCacher()
	if err != nil {
		t.Fatalf("NewCacher: %s", err)
	}

	if err := cache.Put(key, []byte(value)); err != nil {
		t.Fatalf("Put: %s", err)
	}

	v, err := cache.Get(key)
	if err != nil || string(v) != value {
		t.Fatalf("%s %s", v, err)
	}
	t.Logf("%s", v)
}

func TestGet(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "dc1 Host1\ndc2 Host2")
	}))
	defer ts.Close()

	const KEY = "AAAABC"

	h1, err := GetHosts(fmt.Sprintf("%s/", ts.URL), KEY)
	ts.Close()

	t.Logf("%s %s", h1, err)

	h2, err := GetHosts(fmt.Sprintf("%s/", ts.URL), KEY)
	if err != nil {
		t.Fatalf("Repeat Get: %s", err)
	}

	if len(h2) != len(h1) {
		t.Fatalf("%s != %s", h1, h2)
	}

}
