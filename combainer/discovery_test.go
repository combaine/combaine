package combainer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/hosts"
	"github.com/stretchr/testify/assert"
)

var testCache, _ = cache.NewInMemoryCache(nil)
var (
	pluginCfg = configs.PluginConfig{}
	testCtx   = &Context{
		Cache: testCache,
	}
	testCfg = configs.PluginConfig{}
)

func TestRegisterFetcherLoader(t *testing.T) {
	pdf := &PredefineFetcher{}
	f := func(context *Context, config map[string]interface{}) (HostFetcher, error) {
		return pdf, nil
	}
	assert.NoError(t, RegisterFetcherLoader("test", f))
	assert.Error(t, RegisterFetcherLoader("test", f))
	_, err := LoadHostFetcher(testCtx, testCfg)
	assert.Error(t, err)

	testCfg["type"] = "nonRegistered"
	_, err = LoadHostFetcher(testCtx, testCfg)
	assert.Error(t, err)

	testCfg["type"] = "test"
	nf, err := LoadHostFetcher(testCtx, testCfg)
	assert.NoError(t, err)
	assert.Equal(t, nf, pdf)
}

func TestPredefineFetcher(t *testing.T) {
	testCfg = configs.PluginConfig{"type": "predefine", "Clusters": 1}
	_, err := LoadHostFetcher(testCtx, testCfg)
	assert.Error(t, err)
	testCfg = configs.PluginConfig{
		"type": "predefine",
		"Clusters": map[string]map[string][]string{
			"Group1": {
				"DC1": {"dc1-host1", "dc1-host2"},
				"DC2": {"dc2-host1", "dc2-host2"},
			},
		},
	}
	pFetcher, err := LoadHostFetcher(testCtx, testCfg)
	assert.NoError(t, err)
	_, err = pFetcher.Fetch("NotInCache")
	assert.Error(t, err)

	fdc1, err := pFetcher.Fetch("Group1")
	assert.NoError(t, err)
	assert.NotEqual(t, "dc1-host1", fdc1["DC1"][1])
	assert.Equal(t, "dc1-host2", fdc1["DC1"][1])
}

func TestHttpFetcher(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fetch/group1":
			payload := []byte("\n" +
				"notValidHost\n" +
				"\tnotValidHost\n" +
				"dcA\thost1-in-dcA\n" +
				"dcA\thost2-in-dcA\n" +
				"dcB\thost1-in-dcB\n" +
				"dcB\thost2-in-dcB\n")
			w.Write(payload)
		case "/fetch/group1-json":
			payload := []byte(`[
				{ "root_datacenter_name": "dcA" },
				{ "fqdn": "", "root_datacenter_name": "dcA" },
				{ "fqdn": "host1-in-dcA", "root_datacenter_name": "dcA" },
				{ "fqdn": "host2-in-dcA", "root_datacenter_name": "dcA" },
				{ "fqdn": "host1-in-dcB", "root_datacenter_name": "dcB" },
				{ "fqdn": "host2-in-dcB", "root_datacenter_name": "dcB" }
			]`)
			w.Write(payload)
		case "/fetch/group1-qjson":
			payload := []byte(`{
				"group":"group1",
				"children":[
					"dcA-host1-in-dcA",
					"dcA-host2-in-dcA",
					"dcB-host1-in-dcB",
					"dcB-host2-in-dcB"
				]
			}`)
			w.Write(payload)
		case "/fetch/group2":
			w.Write([]byte("\n"))
		case "/fetch/group3":
			w.Write([]byte("some\n"))
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Not Found")
		}
	}))
	defer ts.Close()

	var (
		Failed = true
		Ok     = false
	)

	cases := []struct {
		config  configs.PluginConfig
		query   string
		err     bool
		errType error
	}{
		{configs.PluginConfig{"type": "http", "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr())},
			"Missing Format", Failed, common.ErrMissingFormatSpecifier,
		},
		{configs.PluginConfig{"type": "http", "BasicUrl": "http://non-exists:9898/%s"},
			"NotInCache", Failed, nil,
		},
		{configs.PluginConfig{"type": "http", "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s"},
			"NotInCache", Failed, nil,
		},
		{configs.PluginConfig{"type": "http", "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s"},
			"group1", Ok, nil,
		},
		{
			configs.PluginConfig{
				"type":     "http",
				"Format":   "json",
				"BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s",
			},
			"group1-json", Ok, nil,
		},
		{
			configs.PluginConfig{
				"type":     "http",
				"Format":   "qjson",
				"BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s",
			},
			"group1-qjson", Ok, nil,
		},
	}
	expect := hosts.Hosts{
		"dcA": {"host1-in-dcA", "host2-in-dcA"},
		"dcB": {"host1-in-dcB", "host2-in-dcB"},
	}
	for _, c := range cases {
		hFetcher, err := LoadHostFetcher(testCtx, c.config)
		assert.NoError(t, err, "Failed to load host fetcher")
		resp, err := hFetcher.Fetch(c.query)
		if c.err {
			if c.errType != nil {
				assert.Equal(t, err, c.errType)
			} else {
				assert.Error(t, err, fmt.Sprintf("Test filed for %s", c.query))
			}
		} else {
			assert.Len(t, resp, 2)
			assert.Equal(t, expect, resp)
		}
	}
}
