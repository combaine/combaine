package juggler

import (
	"log"
	"testing"

	lua "github.com/yuin/gopher-lua"
)

var data = DataType{
	"host1": map[string]interface{}{
		"error":    []float64{11133.4},
		"1rps":     111234,
		"1timings": 1110.000,
	},
	"host2": map[string]interface{}{
		"2error":   []float64{22233.4, 222222.2},
		"2rps":     222234,
		"2timings": 2220.000,
	},
	"host3": map[string]interface{}{
		"3error":   []float64{33333.4, 333222.2, 3333434.3},
		"3rps":     333234,
		"3timings": 3330.000,
	},
	"host7": map[string]interface{}{
		"7error":   []float64{777.1, 777.2, 777.3, 777.4, 777.5, 777.6, 777.7},
		"7rps":     777,
		"7timings": 777.777,
	},
}

func BenchmarkToLuaTable(b *testing.B) {
	l := lua.NewState()
	if err := l.DoFile("plugin.lua"); err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		table, err := ToLuaTable(l, data)
		if err != nil {
			log.Fatalln(err)
		}
		l.SetGlobal("table", table)
		l.Push(l.GetGlobal("printtable"))
		l.Call(0, 0)
	}
	l.Close()
}
