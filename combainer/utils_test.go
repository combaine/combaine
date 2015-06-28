package combainer

import (
	"testing"
)

func TestGenSessionId(t *testing.T) {
	id1 := GenerateSessionId()
	id2 := GenerateSessionId()
	t.Logf("%s %s", id1, id2)
	if id1 == id2 {
		t.Fatal("the same id")
	}
}

func BenchmarkGenSessionId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateSessionId()
	}
}
