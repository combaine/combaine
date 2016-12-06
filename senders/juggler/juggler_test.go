package juggler

import (
	"log"
	"testing"

	"github.com/combaine/combaine/common/tasks"
	lua "github.com/yuin/gopher-lua"
)

var data = tasks.DataType{
	"host1": map[string]interface{}{
		"service1": map[string]interface{}{
			"front1.timings": []float64{11133.4},
			"1rps":           111234,
			"1error":         1110.000,
		},
	},
	"host2": map[string]interface{}{
		"service2": map[string]interface{}{
			"front2.timings": []float64{22233.4, 222222.2},
			"2rps":           222234,
			"2error":         2220.000,
		},
	},
	"host3": map[string]interface{}{
		"service3": map[string]interface{}{
			"front3.timings": []float64{33333.4, 333222.2, 3333434.3},
			"3rps":           333234,
			"3error":         3330.000,
		},
	},
	"host7": map[string]interface{}{
		"service4": map[string]interface{}{
			"front7.timings": []float64{777.1, 777.2, 777.3, 777.4, 777.5, 777.6, 777.7},
			"7rps":           777,
			"7error":         777.777,
		},
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

func TestQueryLuaTable(t *testing.T) {

	l := lua.NewState()
	if err := l.DoFile("plugin_test.lua"); err != nil {
		panic(err)
	}
	table, err := dataToLuaTable(l, data)
	if err != nil {
		log.Panic(err)
	}
	l.SetGlobal("query", lua.LString("%S+/%S+timings/3"))
	l.Push(l.GetGlobal("testQuery"))
	l.Push(table)
	l.Call(1, 1)
	result := l.ToTable(1)

	CRIT := 3 // defaultLevel
	events, err := luaResultToJugglerEvents(CRIT, result)
	if err != nil {
		log.Printf("Failed to convert lua table to []jugglerEvent, %s", err)
	}

	for _, j := range events {
		log.Printf("Juggler event: {host: %s, service: %s, description: %s, Level: %d}\n",
			j.Host, j.Service, j.Description, j.Level)
	}
}
