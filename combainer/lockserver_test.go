package combainer

import (
	"testing"

	"github.com/noxiouz/Combaine/common/configs"
)

func TestMain(t *testing.T) {
	cfg := configs.LockServerSection{
		Hosts:   []string{"localhost:2181"},
		Id:      "MyID",
		Name:    "TestName",
		Timeout: 5,
	}
	l, err := NewLockServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}
