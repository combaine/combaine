package common

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/combaine/combaine/common/hosts"
	"github.com/combaine/combaine/repository"
	"github.com/stretchr/testify/assert"
)

var (
	pluginCfg = repository.PluginConfig{}
	testCfg   = repository.PluginConfig{}
)

func TestRegisterFetcherLoader(t *testing.T) {
	pdf := &PredefineFetcher{}
	f := func(config repository.PluginConfig) (HostFetcher, error) {
		return pdf, nil
	}
	assert.NoError(t, RegisterFetcherLoader("test", f))
	assert.Error(t, RegisterFetcherLoader("test", f))
	_, err := LoadHostFetcher(testCfg)
	assert.Error(t, err)

	testCfg["type"] = "nonRegistered"
	_, err = LoadHostFetcher(testCfg)
	assert.Error(t, err)

	testCfg["type"] = "test"
	nf, err := LoadHostFetcher(testCfg)
	assert.NoError(t, err)
	assert.Equal(t, nf, pdf)
}

func TestPredefineFetcher(t *testing.T) {
	testCfg = repository.PluginConfig{"type": "predefine", "Clusters": 1}
	_, err := LoadHostFetcher(testCfg)
	assert.Error(t, err)
	testCfg = repository.PluginConfig{
		"type": "predefine",
		"Clusters": map[string]map[string][]string{
			"Group1": {
				"DC1": {"dc1-host1", "dc1-host2"},
				"DC2": {"dc2-host1", "dc2-host2"},
			},
		},
	}
	pFetcher, err := LoadHostFetcher(testCfg)
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
				"\tHost.in.NoDC\n" +
				"dcA\thost1.in.dcA\n" +
				"dcA\thost2.in.dcA\n" +
				"dcB\thost1.in.dcB\n" +
				"dcB\thost2.in.dcB\n")
			w.Write(payload)
		case "/fetch/group1-json":
			payload := []byte(`[
				{ "root_datacenter_name": "dcA" },
				{ "fqdn": "", "root_datacenter_name": "dcA" },
				{ "fqdn": "host1.in.dcA", "root_datacenter_name": "dcA" },
				{ "fqdn": "host2.in.dcA", "root_datacenter_name": "dcA" },
				{ "fqdn": "host1.in.dcB", "root_datacenter_name": "dcB" },
				{ "fqdn": "host2.in.dcB", "root_datacenter_name": "dcB" }
			]`)
			w.Write(payload)
		case "/fetch/group1-json-rtc_dc1":
			payload := []byte(`{
				"result": [
					{ "hostname": "host1.dc1" },
					{ "hostname": "host2.dc1" }
				]
			}`)
			w.Write(payload)
		case "/fetch/group1-json-rtc_dcB":
			payload := []byte(`{
				"result": [
					{ "hostname": "host1.dcB" },
					{ "hostname": "host2.dcB" }
					{ "hostname": "host3.dcB" }
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
		config  repository.PluginConfig
		query   string
		err     bool
		errType error
		expect  hosts.Hosts
	}{
		{repository.PluginConfig{"type": "http", "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr())},
			"Missing Format", Failed, ErrMissingFormatSpecifier,
			hosts.Hosts{"NoDC": {"Host.in.NoDC"}, "dcA": {"host1.in.dcA", "host2.in.dcA"}, "dcB": {"host1.in.dcB", "host2.in.dcB"}},
		},
		{repository.PluginConfig{"type": "http", "BasicUrl": "http://non-exists:9898/%s"},
			"NotInCache", Failed, nil,
			hosts.Hosts{"dcA": {"host1.in.dcA", "host2.in.dcA"}, "dcB": {"host1.in.dcB", "host2.in.dcB"}},
		},
		{repository.PluginConfig{"type": "http", "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s"},
			"NotInCache", Failed, nil,
			hosts.Hosts{"dcA": {"host1.in.dcA", "host2.in.dcA"}, "dcB": {"host1.in.dcB", "host2.in.dcB"}},
		},
		{repository.PluginConfig{"type": "http", "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s"},
			"group1", Ok, nil,
			hosts.Hosts{"NoDC": {"Host.in.NoDC"}, "dcA": {"host1.in.dcA", "host2.in.dcA"}, "dcB": {"host1.in.dcB", "host2.in.dcB"}},
		},
		{
			repository.PluginConfig{
				"type":     "http",
				"Format":   "json",
				"BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s",
			},
			"group1-json", Ok, nil,
			hosts.Hosts{"dcA": {"host1.in.dcA", "host2.in.dcA"}, "dcB": {"host1.in.dcB", "host2.in.dcB"}},
		},
	}
	for _, c := range cases {
		hFetcher, err := LoadHostFetcher(c.config)
		assert.NoError(t, err, "Failed to load host fetcher")
		resp, err := hFetcher.Fetch(c.query)
		if c.err {
			if c.errType != nil {
				assert.Equal(t, err, c.errType)
			} else {
				assert.Error(t, err, fmt.Sprintf("Test filed for %s", c.query))
			}
		} else {
			assert.Equal(t, c.expect, resp)
		}
	}
}

func TestRTCFetcher(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fetch/group1-json-rtc_dc1":
			payload := []byte(`{
				"result": [
					{ "container_hostname": "host1.dc1" },
					{ "container_hostname": "host2.dc1" }
				]
			}`)
			w.Write(payload)
		case "/fetch/group1-json-rtc_dcB":
			payload := []byte(`{
				"result": [
					{ "container_hostname": "host1.dcB" },
					{ "container_hostname": "host2.dcB" },
					{ "container_hostname": "host3.dcB" }
				]
			}`)
			w.Write(payload)
		case "/fetch/group1-json-rtc-dcC":
			payload := []byte(`{
				"result": [
					{ "container_hostname": "host1.dcC" },
					{ "container_hostname": "host2.dcC" },
					{ "container_hostname": "host3.dcC" }
				]
			}`)
			w.Write(payload)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Not Found")
		}
	}))
	defer ts.Close()

	var (
		Ok = false
	)

	cases := []struct {
		config  repository.PluginConfig
		query   string
		err     bool
		errType error
		expect  hosts.Hosts
	}{
		{repository.PluginConfig{"type": "rtc", "geo": []string{"dc1"}, "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s"},
			"group1-json-rtc", Ok, nil, hosts.Hosts{"dc1": {"host1.dc1", "host2.dc1"}}},
		{repository.PluginConfig{"type": "rtc", "geo": []string{"dc1", "dcB"}, "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s"},
			"group1-json-rtc", Ok, nil,
			hosts.Hosts{"dc1": {"host1.dc1", "host2.dc1"}, "dcB": {"host1.dcB", "host2.dcB", "host3.dcB"}}},
		{repository.PluginConfig{"type": "rtc", "geo": []string{"dcC"}, "Delimiter": "-", "BasicUrl": fmt.Sprintf("http://%s/fetch", ts.Listener.Addr()) + "/%s"},
			"group1-json-rtc", Ok, nil, hosts.Hosts{"dcC": {"host1.dcC", "host2.dcC", "host3.dcC"}}},
	}
	for _, c := range cases {
		hFetcher, err := LoadHostFetcher(c.config)
		assert.NoError(t, err, "Failed to load host fetcher")
		resp, err := hFetcher.Fetch(c.query)
		if c.err {
			if c.errType != nil {
				assert.Equal(t, err, c.errType)
			} else {
				assert.Error(t, err, fmt.Sprintf("Test filed for %s", c.query))
			}
		} else {
			assert.Equal(t, c.expect, resp)
		}
	}
}
