package juggler

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/combaine/combaine/common/tasks"
	"github.com/stretchr/testify/assert"
	lua "github.com/yuin/gopher-lua"
)

var data = tasks.DataType{
	"host1": map[string]interface{}{
		"timings": []float64{11133.4},
		"1rps":    111234,
		"1error":  1110.000,
	},
	"host2": map[string]interface{}{
		"2timings": []float64{22233.4, 222222.2},
		"2rps":     222234,
		"2error":   2220.000,
	},
	"host3": map[string]interface{}{
		"3timings": []float64{33333.4, 333222.2, 3333434.3},
		"3rps":     333234,
		"3error":   3330.000,
	},
	"host7": map[string]interface{}{
		"7timings": []float64{777.1, 777.2, 777.3, 777.4, 777.5, 777.6, 777.7},
		"7rps":     777,
		"7error":   777.777,
	},
}

func BenchmarkDataToLuaTable(b *testing.B) {
	l := lua.NewState()
	if err := l.DoFile("plugin_test.lua"); err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		table, err := dataToLuaTable(l, data)
		if err != nil {
			log.Fatalln(err)
		}
		l.SetGlobal("table", table)
		l.Push(l.GetGlobal("sumTable"))
		l.Push(l.GetGlobal("table"))
		l.Call(1, 1)
		l.Get(1)
		l.Pop(1)
	}
	l.Close()
}

func TestSumLuaTable(t *testing.T) {
	var expected float64
	for _, v := range data {
		for k, iv := range v {
			if strings.HasSuffix(k, "timings") {
				for _, i := range iv.([]float64) {
					expected += i
				}
			} else {
				num, _ := strconv.ParseFloat(fmt.Sprintf("%v", iv), 64)
				expected += num
			}
		}
	}

	l := lua.NewState()
	if err := l.DoFile("plugin_test.lua"); err != nil {
		panic(err)
	}
	table, err := dataToLuaTable(l, data)
	if err != nil {
		log.Fatalln(err)
	}
	l.Push(l.GetGlobal("sumTable"))
	l.Push(table)
	l.Call(1, 1)
	result := l.Get(1)
	assert.Equal(t, fmt.Sprintf("%.5f", expected), fmt.Sprintf("%.5f", result))
}
