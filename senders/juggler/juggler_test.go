package juggler

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
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

	testConf := "testdata/config/juggler_example.yaml"
	os.Setenv("JUGGLER_CONFIG", testConf)
	sConf, err := GetJugglerSenderConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load juggler sender config %s: %s", testConf, err))
	}

	conf.PluginsDir = sConf.PluginsDir
	conf.JHosts = sConf.Hosts
	conf.JFrontend = sConf.Frontend

	return conf
}

// Benchmarks
func BenchmarkDataToLuaTable(b *testing.B) {
	l, err := LoadPlugin("testdata/plugins", "test")
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
		t.Fatalf("Loading non existing plugin should return error")
	}
	if _, err := LoadPlugin("testdata/plugins", "test"); err != nil {
		t.Fatalf("Failed to load plugin 'test': %s", err)
	}
}

func TestPrepareLuaEnv(t *testing.T) {
	jconf := DefaultJugglerTestConfig()
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
	l, err := LoadPlugin("testdata/plugins", "test")
	assert.NoError(t, err)
	table, err := dataToLuaTable(l, data)
	assert.NoError(t, err)
	l.SetGlobal("query", lua.LString(".+/.+/.+timings/3"))
	l.Push(l.GetGlobal("testQuery"))
	l.Push(table)
	l.Call(1, 1)
	result := l.ToTable(1)

	events, err := luaResultToJugglerEvents("OK", result)
	assert.NoError(t, err)
	assert.Len(t, events, 2)
}

func TestGetCheck(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/checks/checks":
			hostName := r.URL.Query().Get("host_name")
			if hostName == "" {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintln(w, "Query parameter host_name not specified")
			}
			fileName := fmt.Sprintf("testdata/checks/%s.json", hostName)
			data, err := ioutil.ReadFile(fileName)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Failed to read file %s, %s", fileName, err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(data)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Not Found")
		}
	}))
	defer ts.Close()

	jconf := DefaultJugglerTestConfig()
	assert.Contains(t, jconf.JHosts[0], "localhost")
	jconf.JHosts = []string{ts.Listener.Addr().String()}
	jconf.JFrontend = []string{ts.Listener.Addr().String()}

	js, err := NewJugglerSender(jconf, "Test ID")
	assert.NoError(t, err)

	cases := []struct {
		name      string
		exists    bool
		len       int
		withFlaps map[string]*JugglerFlapConfig
	}{
		{"backend", true, 5, map[string]*JugglerFlapConfig{
			"api_timings":             nil,
			"prod-app_khttpd_timings": {StableTime: 60, CriticalTime: 90},
			"prod-app_5xx":            nil,
			"common_log_err":          nil,
			"api_5xx":                 nil,
		}},
		{"frontend", true, 4, map[string]*JugglerFlapConfig{
			"wsgi_timings":            nil,
			"prod-app_khttpd_timings": {StableTime: 60, CriticalTime: 90},
			"node_err":                nil,
			"app_5xx":                 nil,
		}},
		{"nonExisting", false, 0, make(map[string]*JugglerFlapConfig)},
	}

	ctx := context.TODO()
	for _, c := range cases {
		js.Host = c.name
		juggler_resp, err := js.getCheck(ctx)
		if c.exists {
			assert.NoError(t, err)
		} else {
			assert.Contains(t, fmt.Sprintf("%v", err), "no such file")
		}
		assert.Len(t, juggler_resp[js.Host], c.len)

		for k, v := range c.withFlaps {
			assert.Equal(t, v, juggler_resp[c.name][k].Flap)
		}
	}
}
