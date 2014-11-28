package combainer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/noxiouz/Combaine/common/hosts"
)

const (
	fetcherCacheNamespace = "simpleFetcherCacheNamespace"
)

type HostFetcher interface {
	Fetch(group string) (hosts.Hosts, error)
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

	if fetcherConfig.Separator == "" {
		fetcherConfig.Separator = "\t"
	}

	f := &SimpleFetcher{
		SimpleFetcherConfig: fetcherConfig,
		Context:             context,
	}
	return f, nil
}

func (s *SimpleFetcher) Fetch(groupname string) (hosts.Hosts, error) {
	url := fmt.Sprintf(s.BasicUrl, groupname)
	resp, err := http.Get(url)
	var body []byte
	if err != nil {
		LogWarning("Unable to fetch hosts from %s: %s. Cache is used", url, err)
		body, err = s.Cache.Get(fetcherCacheNamespace, groupname)
		if err != nil {
			LogErr("Unable to read data from the cache: %s", err)
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
	fetchedHosts := make(hosts.Hosts)
	items := strings.TrimSuffix(string(body), "\n")
	for _, dcAndHost := range strings.Split(items, "\n") {
		temp := strings.Split(dcAndHost, s.Separator)
		if len(temp) != 2 {
			LogInfo("Wrong input string %s", temp)
			continue
		}
		dc, host := temp[0], temp[1]
		fetchedHosts[dc] = append(fetchedHosts[dc], host)
	}
	if len(fetchedHosts) == 0 {
		return fetchedHosts, fmt.Errorf("No hosts")
	}
	return fetchedHosts, nil
}
