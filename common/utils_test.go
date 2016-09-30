package common

import (
	"fmt"
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
		{map[string]struct{}{"key": struct{}{}}, new(map[string]struct{})},
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
		{map[string]struct{}{"key": struct{}{}}, new(map[string]struct{})},
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

func TestMapUpdate(t *testing.T) {
	cases := []struct {
		src      map[string]interface{}
		dst      map[string]interface{}
		expected map[string]interface{}
	}{
		{
			map[string]interface{}{"x": 1, "z": 3},
			map[string]interface{}{"y": 2, "z": 4},
			map[string]interface{}{"x": 1, "y": 2, "z": 3}},
		{
			map[string]interface{}{"a": "a", "b": "b"},
			map[string]interface{}{},
			map[string]interface{}{"a": "a", "b": "b"}},
		{
			map[string]interface{}{"a": 1, "b": 2},
			map[string]interface{}{"a": "a", "b": false, "c": 3},
			map[string]interface{}{"a": 1, "b": 2, "c": 3}},
	}
	for _, c := range cases {
		MapUpdate(c.src, c.dst)

		assert.Equal(t, c.expected, c.dst)
	}
}

func TestInterfaceToString(t *testing.T) {
	cases := []struct {
		v        interface{}
		expected string
	}{
		{uint(1), "1"},
		{uint8(2), "2"},
		{float32(2), fmt.Sprintf("%f", float32(2))},
		{int64(0), "0"},
		{[]byte("a\nb"), "\"a\\nb\""},
		{[]string{"a", "|", "b"}, "[a | b]"},
	}

	for _, c := range cases {
		assert.Equal(t, InterfaceToString(c.v), c.expected)
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
