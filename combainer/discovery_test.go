package combainer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/configs"
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
			payload := strings.Join([]string{
				"",
				"notValidHost",
				"\tnotValidHost",
				"DC1\tdc1-host1",
				"DC1\tdc1-host2",
				"DC2\tdc2-host1",
				"DC2\tdc2-host2",
			}, "\n")
			fmt.Fprint(w, payload)
		case "/fetch/group2":
			fmt.Fprintln(w, "")
		case "/fetch/group3":
			fmt.Fprintln(w, "some")
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Not Found")
		}
	}))
	defer ts.Close()

	testCfg = configs.PluginConfig{"type": "http", "BasicUrl": 1}
	_, err := LoadHostFetcher(testCtx, testCfg)
	assert.Error(t, err)

	testCfg = configs.PluginConfig{
		"type":     "http",
		"BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()),
	}
	hFetcher, err := LoadHostFetcher(testCtx, testCfg)
	assert.NoError(t, err)
	_, err = hFetcher.Fetch("Missing Format")
	assert.Equal(t, err, common.ErrMissingFormatSpecifier)

	testCfg = configs.PluginConfig{
		"type":     "http",
		"BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s",
	}
	hFetcher, err = LoadHostFetcher(testCtx, testCfg)
	assert.NoError(t, err)
	_, err = hFetcher.Fetch("NotInCache")
	assert.Error(t, err)

	fdc1, err := hFetcher.Fetch("group1")
	// only 2 datacenter should be present in parsed result
	assert.Len(t, fdc1, 2)
	t.Logf("Hosts from cacahe %v", fdc1)
	assert.NoError(t, err)
	assert.NotEqual(t, "dc1-host1", fdc1["DC1"][1])
	assert.Equal(t, "dc1-host2", fdc1["DC1"][1])

	hosts, err := hFetcher.Fetch("group2")
	assert.Equal(t, err, common.ErrNoHosts)
	assert.Len(t, hosts, 0)

	hosts, err = hFetcher.Fetch("group3")
	assert.Error(t, err)
	assert.Len(t, hosts, 0)

	testCfg = configs.PluginConfig{
		"type":     "http",
		"BasicUrl": "http://not-exists:9999/fetch/%s",
	}
	hFetcher, err = LoadHostFetcher(testCtx, testCfg)
	hosts, err = hFetcher.Fetch("miss")
	assert.Error(t, err)
	assert.Len(t, hosts, 0)
}
