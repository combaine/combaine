package juggler

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	lua "github.com/yuin/gopher-lua"
)

func TestPluginSimple(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	jconf := DefaultJugglerTestConfig()
	jconf.OK = nil
	jconf.INFO = nil
	jconf.WARN = nil
	jconf.CheckName = "Matcher"
	jconf.Variables = map[string]string{
		"VAR_FIRST": "iftimeofday(23, 7, 10000 , 4000)",
		"VAR1":      "300",
		"VAR2":      "iftimeofday(20, 6, 10, 40)",
		"VAR_LAST":  "iftimeofday(1, 6, 2600, 2800)",
		"VAR_LAST2": "3000",
	}
	jconf.PluginsDir = "../../plugins/juggler"
	jconf.Plugin = "simple"
	jconf.Host = "hostname_from_config"
	jconf.JPluginConfig = map[string]interface{}{
		"history":             3,
		"withoutDefaultState": true,
	}

	js, err := NewSender(jconf, "Test ID")
	assert.NoError(t, err)

	cases := []struct {
		checks     []string
		expectFire bool
	}{
		{[]string{"${agg}['a'] >= VAR_FIRST"}, true},
		{[]string{"${agg}['a'] >= VAR1"}, true},

		{[]string{"${agg}['b'] >= 1"}, true},

		{[]string{"${agg}['t'][4] > VAR2"}, false},
		{[]string{"${agg}['t'][1] > 444"}, false},

		{[]string{"${agg}['a'] + ${agg}['b'] <=8"}, false},
		{[]string{"${agg}['a'] - ${agg}['b'] - ${agg}['c'] <13009"}, true},
		{[]string{"${agg}['a']-${agg}['b']-${agg}['b_c.a.c-d'] <10",
			"${agg}['a']-${agg}['b']-${agg}['c-d'] <10011"}, true},
		{[]string{"${agg}['a'] - (${agg}['b'] + ${agg}['c'])>=312"}, true},
		{[]string{"${agg}['b'] + ${agg}['c'] + ${agg}['d'] - ${agg}['e'] > 113"}, true},

		{[]string{"(${agg}['d'] <= 114 )"}, true},
		/// ???
		{[]string{"(${agg}['j']['q']['p']+${agg}['r']['s']['t'])/(${agg}['x']['y']['z']+0.01)>0.15"}, true},
		{[]string{"(${agg}['a']+${agg}['b']+${agg}['c']+${agg}['d'])/${agg}['f']>0.16"}, true},

		{[]string{"(${agg}['b'] + ${agg}['c'] + ${agg}['d'] + ${agg}['e'])/${agg}['f'] > 0.17"}, true},
		{[]string{"${agg}['f'] / (${agg}['b'] + ${agg}['c'] + ${agg}['d'] + ${agg}['e']) > 0.18",
			"${agg}['f'] / (${agg}['b'] + ${agg}['c'] + ${agg}['D'] + ${agg}['Q']) > 0.19"}, true},

		// and/or + -1
		{[]string{"${agg}['t'][0]>=0.1 or ${agg}['t'][-1]>=1.020"}, true},
		{[]string{"${agg}['t'][0]>=0.5 or ${agg}['t'][-1]>=1.521"}, true},
		{[]string{"${agg}['t'][0]>=0.1 or ${agg}['t'][-1]>=1.022"}, true},
		{[]string{"${agg}['t'][0]>=0.5 or ${agg}['t'][-1]>=1.523"}, true},

		{[]string{"${agg}['t'][0]<0.3 and ${agg}['t'][-1]<3.024"}, true},
		{[]string{"${agg}['t2'][0]<0.1 and ${agg}['t2'][-1]<0.925"}, false},
		{[]string{"${agg}['t'][0]<0.1 and ${agg}['t'][-1]<1.026"}, false},
		{[]string{"${agg}['t'][0]<0.5 and ${agg}['t'][-1]<3.527"}, true},
		{[]string{"${agg}['bt'][4]<3000 and ${agg}['bt2'][8]<3028"}, true},
		{[]string{"${agg}['bt'][4]<2000 and ${agg}['bt2'][8]<3029"}, false},
		{[]string{"${agg}['bt'][4]<2000 and ${agg}['bt2'][8]<3030",
			"${agg}['bt'][4]<VAR_LAST and ${agg}['bt2'][8]<VAR_LAST2"}, true},
	}

	for _, c := range cases {
		js.CRIT = c.checks
		l, err := LoadPlugin("Test Id", js.PluginsDir, js.Plugin, true)
		assert.NoError(t, err)
		if err != nil {
			t.FailNow()
		}
		js.state = l
		assert.NoError(t, js.preparePluginEnv(globalTestTask))

		events, err := js.runPlugin()
		assert.NoError(t, err)
		t.Logf("%v: %v", c, events)
		actualFire := false
		for _, e := range events {
			if e.Status == "CRIT" {
				actualFire = true
			}
		}
		assert.Equal(t, c.expectFire, actualFire, fmt.Sprintf("%s", c.checks))
	}
}

func TestToLuaValueStruct(t *testing.T) {
	l := lua.NewState()
	data := struct {
		foo string
		Bar []string
	}{foo: "string", Bar: []string{"bar1", "bar2", "bar3"}}

	_, err := toLuaValue(l, &data, dumperToLuaValue)
	assert.Error(t, err) // Unexpected pointer
	resp, err := toLuaValue(l, data, dumperToLuaValue)
	assert.NoError(t, err)
	bar := resp.(*lua.LTable).RawGet(lua.LString("Bar")).(*lua.LTable)
	assert.NotEqual(t, lua.LNil, bar)
	assert.Equal(t, bar.Len(), 3)
}

func BenchmarkPassGoLogger(b *testing.B) {
	logrus.SetOutput(ioutil.Discard)
	l := lua.NewState()
	PreloadTools("BenchPassGoLog", true, l)
	fn, err := l.LoadString(`
		local log = require("log");
		log.info("test %s %d %0.3f", "log", 10, 0.33)
	`)
	if err != nil {
		b.Fatalf("Failed to load benchmark: %s", err)
	}
	for i := 0; i < b.N; i++ {
		l.Push(fn)
		l.Call(0, 0)
	}
}

func BenchmarkStringFmtLogger(b *testing.B) {
	logrus.SetOutput(ioutil.Discard)
	l := lua.NewState()
	PreloadTools("BenchStringFmtLog", true, l)
	fn, err := l.LoadString(`
		local log = require("log");
		local fmt = string.format
		log.info(fmt("test %s %d %0.3f", "log", 10, 0.33))
	`)
	if err != nil {
		b.Fatalf("Failed to load benchmark: %s", err)
	}
	for i := 0; i < b.N; i++ {
		l.Push(fn)
		l.Call(0, 0)
	}
}

/* Benchmarks go 2 lua converter
// NewTable vs CreateTable with proper capacity
// NewTable
goos: linux
goarch: amd64
pkg: github.com/combaine/combaine/senders/juggler
BenchmarkDataToLuaTable-4    	    2000	    557000 ns/op	  306858 B/op	    3196 allocs/op
BenchmarkPassGoLogger-4      	  200000	      6096 ns/op	    3172 B/op	      33 allocs/op
BenchmarkStringFmtLogger-4   	  200000	      6695 ns/op	    3268 B/op	      35 allocs/op
PASS
ok  	github.com/combaine/combaine/senders/juggler	4.084s

// CreateTable
goos: linux
goarch: amd64
pkg: github.com/combaine/combaine/senders/juggler
BenchmarkDataToLuaTable-4    	    3000	    467063 ns/op	  109908 B/op	    3090 allocs/op
BenchmarkPassGoLogger-4      	  200000	      6078 ns/op	    3172 B/op	      33 allocs/op
BenchmarkStringFmtLogger-4   	  200000	      6584 ns/op	    3268 B/op	      35 allocs/op
PASS
ok  	github.com/combaine/combaine/senders/juggler	4.303s
*/
func BenchmarkDataToLuaTable(b *testing.B) {
	l, err := LoadPlugin("Test Id", "testdata/plugins", "test", true)
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		table, err := dataToLuaTable(l, globalTestTask.Data)
		if err != nil {
			b.Fatal(err)
		}
		l.SetGlobal("table", table)
		l.Push(l.GetGlobal("sumTable"))
		l.Push(l.GetGlobal("table"))
		l.Call(1, 1)
		l.Pop(1)
	}
	l.Close()
}
