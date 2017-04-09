package common

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeEncode(t *testing.T) {
	cases := []struct {
		source interface{}
		target interface{}
	}{
		{map[string]int{"key": 1}, new(map[string]int)},
		{map[string]string{"key": "okay"}, new(map[string]string)},
		{map[string]struct{}{"key": {}}, new(map[string]struct{})},
		{[]string{"A", "B"}, new([]string)},
		{[]byte{'A', 'B', 0x0, '\t', '\n'}, new([]byte)},
	}

	for _, c := range cases {
		data, err := Encode(c.source)
		assert.NoError(t, err)

		err = Decode(data, c.target)
		assert.NoError(t, err)
		dst := fmt.Sprintf("%v", reflect.ValueOf(c.target).Elem())
		assert.Equal(t, fmt.Sprintf("%v", c.source), dst)
	}
}

func TestPackUnpack(t *testing.T) {
	cases := []struct {
		source interface{}
		target interface{}
	}{
		{map[string]int{"key": 1}, new(map[string]int)},
		{map[string]string{"key": "okay"}, new(map[string]string)},
		{map[string]struct{}{"key": {}}, new(map[string]struct{})},
		{[]string{"A", "B"}, new([]string)},
		{[]byte{'A', 'B', 0x0, '\t', '\n'}, new([]byte)},
	}

	for _, c := range cases {
		data, err := Pack(c.source)
		assert.NoError(t, err)

		err = Unpack(data, c.target)
		assert.NoError(t, err)
		dst := fmt.Sprintf("%v", reflect.ValueOf(c.target).Elem())
		assert.Equal(t, fmt.Sprintf("%v", c.source), dst)
	}
}

func TestGetType(t *testing.T) {
	cases := []struct {
		cfg       map[string]interface{}
		expected  string
		withError bool
	}{
		{map[string]interface{}{"type": 1}, "type field isn't string", true},
		{map[string]interface{}{"type": "okay"}, "okay", false},
		{map[string]interface{}{}, "Missing field type", true},
	}

	for _, c := range cases {
		resp, err := GetType(c.cfg)
		if c.withError {
			assert.Error(t, err)
			resp = err.Error()
		} else {
			assert.Nil(t, err)
		}
		assert.Equal(t, c.expected, resp)
	}
}

func TestInterfaceToString(t *testing.T) {
	cases := []struct {
		v        interface{}
		expected string
	}{
		{uint(1), "1"},
		{uint8(2), "2"},
		{float32(2), "2"},
		{int64(0), "0"},
		{[]byte("a\nb"), "\"a\\nb\""},
		{[]string{"a", "|", "b"}, "[a | b]"},
	}

	for _, c := range cases {
		assert.Equal(t, InterfaceToString(c.v), c.expected)
	}

}

func TestGetSubgroupName(t *testing.T) {
	cases := []struct {
		tags      map[string]string
		expected  string
		withError bool
	}{
		{map[string]string{
			"type": "datacenter", "metahost": "frontend", "name": "DC2",
		}, "frontend-DC2", false},
		{map[string]string{
			"type": "host", "metahost": "frontend", "name": "host.name.one",
		}, "host.name.one", false},
		{map[string]string{
			"type": "metahost", "metahost": "frontend", "name": "frontend",
		}, "frontend", false},
		{map[string]string{"type": "datacenter", "metahost": "a"}, "", true},
		{map[string]string{"metahost": "a", "name": "dc1"}, "", true},
		{map[string]string{"type": "datacenter", "name": "dc1"}, "", true},
	}

	for _, c := range cases {
		name, err := GetSubgroupName(c.tags)
		if c.withError {
			assert.Error(t, err)
		}
		assert.Equal(t, c.expected, name)
	}

}

func TestNameStack(t *testing.T) {
	var push = true
	var pop = false

	cases := []struct {
		name     string
		expected string
		add      bool
	}{
		{"a", "a", push},
		{"b", "a.b", push},
		{"b", "a", pop},
		{"a", "a.a", push},
		{"b", "a.a.b", push},
		{"c", "a.a.b.c", push},
		{"c", "a.a.b", pop},
		{"b", "a.a", pop},
	}

	ns := new(NameStack)

	for _, c := range cases {
		if c.add {
			ns.Push(c.name)
		} else {
			n := ns.Pop()
			assert.Equal(t, c.name, n)
		}
		assert.Equal(t, strings.Join(*ns, "."), c.expected)
	}
}

func TestGetRandomString(t *testing.T) {
	cases := []struct {
		hosts []string
		empty bool
	}{
		{[]string{}, true},
		{[]string{"host1", "host2", "host3", "host4", "host5", "hosts6"}, false},
	}

	for _, c := range cases {
		if c.empty {
			assert.Equal(t, "", GetRandomString(c.hosts))
		} else {
			cur, prev := "", ""
			random := 0
			for i := 0; i < 10; i++ {
				cur = GetRandomString(c.hosts)
				assert.NotEqual(t, "", cur)
				if i != 0 && cur != prev {
					random++
				}
				prev = cur
			}
			if random < 3 {
				log.Fatal("random very predictable")
			}
			log.Printf("Got %d random hosts", random)
		}
	}
}

func TestGenSessionID(t *testing.T) {
	id1 := GenerateSessionID()
	id2 := GenerateSessionID()
	t.Logf("%s %s", id1, id2)
	if id1 == id2 {
		t.Fatal("the same id")
	}
}

func BenchmarkGenSessionID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateSessionID()
	}
}
