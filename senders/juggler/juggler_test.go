package juggler

import (
	"errors"
	"log"
	"testing"

	"github.com/combaine/combaine/common/tasks"
	"github.com/stretchr/testify/assert"
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

func DefaultJugglerTestConfig() *JugglerConfig {
	conf := DefaultJugglerConfig()
	// add test conditions
	conf.Conditions = Conditions{
		OK:   []string{"${nginx}.get('5xx', 0)<0.06"},
		CRIT: []string{"${nginx}.get('5xx', 0)>0.06"},
	}
	// add test config
	conf.JPluginConfig = map[string]interface{}{
		"checks": map[string]interface{}{
			"testTimings": map[string]interface{}{
				"query": "%S+/%S+timings/3",
				"limit": 200.0, // ms
			},
			"testErrors": map[string]interface{}{
				"query": "%S+/%S+error",
				"limit": 50,
			},
		},
	}
	return conf
}

// Benchmarks
func BenchmarkDataToLuaTable(b *testing.B) {
	l, err := LoadPlugin("./plugins", "test")
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		table, err := dataToLuaTable(l, data)
		if err != nil {
			b.Fatal(err)
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

// Tests
func TestLoadPlugin(t *testing.T) {
	if _, err := LoadPlugin(".", "file_not_exists.lua"); err == nil {
		log.Fatalf("Loading non existing plugin should return error")
	}
	if _, err := LoadPlugin("./plugins", "test"); err != nil {
		log.Fatalf("Failed to load plugin 'test': %s", err)
	}
}

func TestPrepareLuaEnv(t *testing.T) {
	jconf := DefaultJugglerTestConfig()
	jconf.PluginsDir = "./plugins"
	jconf.Plugin = "test"

	l, err := LoadPlugin(jconf.PluginsDir, jconf.Plugin)
	assert.NoError(t, err)
	js, err := NewJugglerSender(jconf, "Test ID")
	assert.NoError(t, err)

	js.state = l
	js.preparePluginEnv(data)

	l.Push(l.GetGlobal("testEnv"))
	l.Call(0, 1)

	result := l.ToString(1)
	assert.Equal(t, "OK", result)
}

func TestRunPlugin(t *testing.T) {
	jconf := DefaultJugglerTestConfig()
	jconf.PluginsDir = "./plugins"

	js, err := NewJugglerSender(jconf, "Test ID")
	assert.NoError(t, err)

	jconf.Plugin = "correct"
	l, err := LoadPlugin(jconf.PluginsDir, jconf.Plugin)
	assert.NoError(t, err)
	js.state = l
	assert.NoError(t, js.preparePluginEnv(data))

	_, err = js.runPlugin()
	assert.NoError(t, err)

	jconf.Plugin = "incorrect"
	l, err = LoadPlugin(jconf.PluginsDir, jconf.Plugin)
	assert.NoError(t, err)
	js.state = l
	assert.NoError(t, js.preparePluginEnv(data))
	_, err = js.runPlugin()
	if err == nil {
		err = errors.New("incorrect should fail")
	}
	assert.Contains(t, err.Error(), "Expected 'run' function inside plugin")
}

func TestQueryLuaTable(t *testing.T) {
	l, err := LoadPlugin("plugins", "test")
	assert.NoError(t, err)
	table, err := dataToLuaTable(l, data)
	assert.NoError(t, err)
	l.SetGlobal("query", lua.LString("%S+/%S+timings/3"))
	l.Push(l.GetGlobal("testQuery"))
	l.Push(table)
	l.Call(1, 1)
	result := l.ToTable(1)

	events, err := luaResultToJugglerEvents("CRIT", result)
	if err != nil {
		log.Printf("Failed to convert lua table to []jugglerEvent, %s", err)
	}

	for _, j := range events {
		log.Printf("Juggler event: {host: %s, service: %s, description: %s, Level: %d}\n",
			j.Host, j.Service, j.Description, j.Level)
	}
}
