package juggler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPluginSimple(t *testing.T) {
	jconf := DefaultJugglerTestConfig()
	jconf.OK = []string{}
	jconf.INFO = []string{}
	jconf.WARN = []string{}
	jconf.CRIT = []string{}
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
		{[]string{"sum([v for k,v in ${agg}.iteritems() if (k.startswith('iter') and k.endswith('d'))]) / sum([v for k,v in ${agg}.iteritems() if (k.startswith('items') and k.endswith('d'))]) > 0.5"}, true},
	}

	for _, c := range cases {
		js.CRIT = c.checks
		l, err := LoadPlugin("Test Id", js.PluginsDir, js.Plugin)
		assert.NoError(t, err)
		if err != nil {
			t.FailNow()
		}
		js.state = l
		assert.NoError(t, js.preparePluginEnv(data))

		events, err := js.runPlugin()
		assert.NoError(t, err)
		t.Logf("%v: %v", c, events)
		actualFire := false
		for _, e := range events {
			if e.Level == "CRIT" {
				actualFire = true
			}
		}
		assert.Equal(t, c.expectFire, actualFire, fmt.Sprintf("%s", c.checks))
	}
}
