package combainer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/mitchellh/mapstructure"
)

const (
	fetcherCacheNamespace = "simpleFetcherCacheNamespace"
)

type Hosts map[string][]string

type HostFetcher interface {
	Fetch(group string) (Hosts, error)
}

func (h *Hosts) AllHosts() []string {
	hosts := make([]string, 0)
	for _, hostsInDc := range *h {
		hosts = append(hosts, hostsInDc...)
	}
	return hosts
}

type SimpleFetcher struct {
	SimpleFetcherConfig
	*Context
}

type SimpleFetcherConfig struct {
	Separator string
	BasicUrl  string
}

func NewSimpleFetcher(context *Context, config map[string]interface{}) (HostFetcher, error) {
	var fetcherConfig SimpleFetcherConfig
	if err := mapstructure.Decode(config, &fetcherConfig); err != nil {
		return nil, err
	}

	f := &SimpleFetcher{
		SimpleFetcherConfig: fetcherConfig,
		Context:             context,
	}
	return f, nil
}

func (s *SimpleFetcher) Fetch(groupname string) (Hosts, error) {
	url := fmt.Sprintf(s.BasicUrl, groupname)
	resp, err := http.Get(url)
	var body []byte
	if err != nil {
		LogErr("Unable to fetch hosts from %s: %s. Cache is used", url, err)
		body, err = s.Cache.Get(fetcherCacheNamespace, groupname)
		if err != nil {
			return nil, err
		}
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			LogErr("%s answered with %s. Cache is used", url, resp.Status)
			body, err = s.Cache.Get(fetcherCacheNamespace, groupname)
			if err != nil {
				return nil, err
			}
		} else {
			body, _ = ioutil.ReadAll(resp.Body)
			if put_err := s.Cache.Put(fetcherCacheNamespace, groupname, body); put_err != nil {
				LogInfo("Put error: %s", put_err)
			}
		}
	}

	// Body parsing
	hosts := make(Hosts)
	items := strings.TrimSuffix(string(body), "\n")
	for _, dcAndHost := range strings.Split(items, "\n") {
		temp := strings.Split(dcAndHost, s.Separator)
		if len(temp) != 2 {
			LogInfo("Wrong input string %s", temp)
			continue
		}
		dc, host := temp[0], temp[1]
		hosts[dc] = append(hosts[dc], host)
	}
	if len(hosts) == 0 {
		return hosts, fmt.Errorf("No hosts")
	}
	return hosts, nil
}
