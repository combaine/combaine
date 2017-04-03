package lockserver

import (
	"fmt"
	"strings"
	"testing"

	"github.com/combaine/combaine/common/configs"
)

func TestNewLockServer(t *testing.T) {
	cfg := configs.LockServerSection{
		Hosts:   []string{"localhost:2181"},
		ID:      "MyID",
		Name:    "TestName",
		Timeout: 5,
	}
	l, err := NewLockServer(cfg)
	if err != nil {
		if !strings.Contains(fmt.Sprintf("%s", err), "attempts limit is reached") {
			t.Fatal(err)
		}
		t.Log(err)
	} else {
		l.Close()
	}
}
