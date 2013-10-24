package combainer

import (
	"testing"
)

func TestLS(t *testing.T) {
	t.Log(getParsings())
}

func TestGetParsingConf(t *testing.T) {
	cl := NewClient(COMBAINER_PATH)
	res := cl.readParsingConfig("/etc/combaine/parsing/photo_proxy.json")
	t.Log(res.Groups)
	t.Log(res.Agg_configs)
}

func TestClient(t *testing.T) {
	cl := NewClient(COMBAINER_PATH)
	t.Log(cl)
}
