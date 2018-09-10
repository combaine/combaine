package juggler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var commonJPluginConfig = map[string]interface{}{
	"checks": map[string]interface{}{
		"testTimings": map[string]interface{}{
			"type":       "metahost",
			"query":      ".+_timings$",
			"percentile": 6, // 97
			"status":     "WARN",
			"limit":      0.900, // second
		},
		"testErr": map[string]interface{}{
			"type":   "metahost",
			"query":  "[4e][xr][xr]$",
			"status": "CRIT",
			"limit":  30,
		},
		"test2xx": map[string]interface{}{
			"type":   "metahost",
			"query":  "2xx$",
			"status": "CRIT",
			"limit":  2000,
		},
	},
}

func TestGetCheck(t *testing.T) {
	jconf := DefaultJugglerTestConfig()
	jconf.JHosts = []string{"localhost:3333"}
	jconf.JFrontend = []string{ts.Listener.Addr().String()}

	js, err := NewSender(jconf, "Test ID")
	assert.NoError(t, err)
	_, err = js.getCheck(context.TODO(), []jugglerEvent{})
	assert.Error(t, err)

	jconf.JHosts = []string{"localhost:3333", ts.Listener.Addr().String()}
	js, err = NewSender(jconf, "Test ID")
	cases := []struct {
		name      string
		exists    bool
		len       int
		withFlaps map[string]*jugglerFlapConfig
	}{
		{"hostname_from_config", true, 5, map[string]*jugglerFlapConfig{
			"type1_timings":  nil,
			"type2_timings":  {StableTime: 60, CriticalTime: 90},
			"prod-app_5xx":   nil,
			"common_log_err": nil,
			"api_5xx":        nil,
		}},
		{"frontend", true, 4, map[string]*jugglerFlapConfig{
			"upstream_timings":      nil,
			"ssl_handshake_timings": {StableTime: 60, CriticalTime: 90},
			"4xx":                   nil,
			"2xx":                   nil,
		}},
		{"withUnmarshalError", true, 1, map[string]*jugglerFlapConfig{
			"checkName": {CriticalTime: 90},
		}},
		{"nonExisting", false, 0, make(map[string]*jugglerFlapConfig)},
	}

	ctx := context.TODO()
	for _, c := range cases {
		js.Host = c.name
		jugglerResp, err := js.getCheck(ctx, []jugglerEvent{})
		if c.exists {
			assert.NoError(t, err)
		} else {
			assert.Contains(t, fmt.Sprintf("%v", err), "no such file")
		}
		assert.Len(t, jugglerResp[js.Host], c.len)

		for k, v := range c.withFlaps {
			assert.Equal(t, v, jugglerResp[c.name][k].Flap)
		}
	}
}

func TestEnsureCheck(t *testing.T) {
	cases := []struct {
		name      string
		tags      map[string][]string
		withError bool
	}{
		{"hostname_from_config", map[string][]string{
			"type2_timings":  {"app", "combaine"},
			"common_log_err": {"common", "combaine"},
		}, false},
		{"frontend", map[string][]string{
			"ssl_handshake_timings": {"app", "front", "core", "combaine"},
			"4xx":                   {"combaine"},
		}, false},
		{"backend", map[string][]string{
			"2xx": {"Yep", "app", "back", "core", "combaine"},
		}, false},
		{"missing", map[string][]string{}, true},
	}

	jconf := DefaultJugglerTestConfig()
	jconf.Flap = &jugglerFlapConfig{Enable: 1, StableTime: 60}
	jconf.JPluginConfig = commonJPluginConfig

	jconf.JHosts = []string{ts.Listener.Addr().String()}
	jconf.JFrontend = []string{ts.Listener.Addr().String()}
	jconf.Plugin = "test_ensure_check"

	js, err := NewSender(jconf, "Test ID")
	assert.NoError(t, err)

	state, err := LoadPlugin("Test Id", js.PluginsDir, js.Plugin)
	assert.NoError(t, err)
	js.state = state
	assert.NoError(t, js.preparePluginEnv(data))

	jEvents, err := js.runPlugin()
	assert.NoError(t, err)
	t.Logf("juggler events: %#v", jEvents)

	ctx := context.TODO()
	for _, c := range cases {
		js.Host = c.name
		checks, err := js.getCheck(ctx, []jugglerEvent{})
		t.Logf("juggler checks: %#v", checks)
		if c.withError {
			assert.Error(t, err)
			continue
		}
		if c.name == "backend" {
			// remove first tag, ensureCheck should add it again
			js.Tags = c.tags["2xx"][1:]
			js.ChecksOptions["2xx"] = jugglerFlapConfig{
				Enable: 1, StableTime: 10, CriticalTime: 3000,
			}
		}
		assert.NoError(t, err)
		checks["nonExistingHost"] = map[string]jugglerCheck{"nonExistingCheck": {}}
		assert.NoError(t, js.ensureCheck(ctx, checks, jEvents))
		for service, tags := range c.tags {
			assert.Equal(t, tags, checks[c.name][service].Tags, fmt.Sprintf("host %s servce %s", c.name, service))
		}
		// reset tags here for coverage purpose
		js.Tags = []string{}
		js.ChecksOptions = map[string]jugglerFlapConfig{}
	}
	// non existing check check
	js.Host = "someHost"
	checks := map[string]map[string]jugglerCheck{"nonExistingHost": {
		"nonExistingCheck": jugglerCheck{},
	}}
	assert.NoError(t, js.ensureCheck(ctx, checks, jEvents))
}

func TestSendBatch(t *testing.T) {
	jconf := DefaultJugglerTestConfig()

	jconf.Aggregator = "timed_more_than_limit_is_problem"
	jconf.AggregatorKWArgs = interface{}(map[string]interface{}{
		"nodata_mode": "force_ok",
		"limits": []map[string]interface{}{
			{"crit": "146%", "day_start": 1, "day_end": 7, "time_start": 20, "time_end": 8},
		},
	})

	jconf.JPluginConfig = commonJPluginConfig
	jconf.JHosts = []string{ts.Listener.Addr().String()}
	jconf.BatchSize = 2
	jconf.BatchEndpoint = fmt.Sprintf("http://%s/events", ts.Listener.Addr().String())
	jconf.Plugin = "test_ensure_check"

	cases := []string{"hostname_from_config", "deadline", "frontend"}
	for _, c := range cases {
		jconf.Host = c
		if c == "deadline" {
			js, err := NewSender(jconf, "Test ID")
			assert.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 1)
			assert.Contains(t, fmt.Sprintf("%s", js.Send(ctx, data)), context.DeadlineExceeded.Error())
			cancel()
		} else {
			js, err := NewSender(jconf, "Test ID")
			assert.NoError(t, err)
			err = js.Send(context.TODO(), data)
			//assert.Contains(t, fmt.Sprintf("%s", err), "getsockopt: connection refused")
			assert.Contains(t, fmt.Sprintf("%s", err), "failed to send 1/8 events")
		}
	}
}

func TestSendEvent(t *testing.T) {
	jconf := DefaultJugglerTestConfig()

	jconf.Aggregator = "timed_more_than_limit_is_problem"
	jconf.AggregatorKWArgs = interface{}(map[string]interface{}{
		"nodata_mode": "force_ok",
		"limits": []map[string]interface{}{
			{"crit": "146%", "day_start": 1, "day_end": 7, "time_start": 20, "time_end": 8},
		},
	})

	jconf.JPluginConfig = commonJPluginConfig
	jconf.JHosts = []string{"localhost:3333", ts.Listener.Addr().String()}
	jconf.JFrontend = []string{"localhost:3333", ts.Listener.Addr().String()}
	jconf.Plugin = "test_ensure_check"

	cases := []string{"hostname_from_config", "deadline", "frontend"}
	for _, c := range cases {
		jconf.Host = c
		if c == "deadline" {
			js, err := NewSender(jconf, "Test ID")
			assert.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 1)
			assert.Contains(t, fmt.Sprintf("%s", js.Send(ctx, data)), context.DeadlineExceeded.Error())
			cancel()
		} else {
			js, err := NewSender(jconf, "Test ID")
			assert.NoError(t, err)
			err = js.Send(context.TODO(), data)
			//assert.Contains(t, fmt.Sprintf("%s", err), "getsockopt: connection refused")
			assert.Contains(t, fmt.Sprintf("%s", err), "failed to send 8/16 events")
		}
	}
}
