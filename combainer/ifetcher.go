package combainer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/mitchellh/mapstructure"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/hosts"
	"github.com/noxiouz/Combaine/common/httpclient"
)

func init() {
	if err := RegisterFetcherLoader("http", newHttpFetcher); err != nil {
		panic(err)
	}
	if err := RegisterFetcherLoader("predefine", newPredefineFetcher); err != nil {
		panic(err)
	}
}

const (
	fetcherCacheNamespace = "simpleFetcherCacheNamespace"
)

type FetcherLoader func(*Context, map[string]interface{}) (HostFetcher, error)

var (
	fetchers   map[string]FetcherLoader = make(map[string]FetcherLoader)
	httpClient                          = httpclient.NewClientWithTimeout(1*time.Second, 3*time.Second)
)

func RegisterFetcherLoader(name string, f FetcherLoader) error {
	_, ok := fetchers[name]
	if ok {
		return fmt.Errorf("HostFetcher `%s` is already registered", name)
	}

	fetchers[name] = f
	return nil
}

func LoadHostFetcher(context *Context, config configs.PluginConfig) (HostFetcher, error) {
	name, err := config.Type()
	if err != nil {
		return nil, fmt.Errorf("unable to get type of HostFetcher: %s", err)
	}

	initializer, ok := fetchers[name]
	if !ok {
		return nil, fmt.Errorf("HostFetcher `%s` isn't registered", name)
	}

	return initializer(context, config)
}

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

type PredefineFetcher struct {
	mutex sync.Mutex
	PredefineFetcherConfig
}

type PredefineFetcherConfig struct {
	Clusters map[string]hosts.Hosts
}

func newPredefineFetcher(_ *Context, config map[string]interface{}) (HostFetcher, error) {
	var fetcherConfig PredefineFetcherConfig
	if err := mapstructure.Decode(config, &fetcherConfig); err != nil {
		return nil, err
	}

	f := &PredefineFetcher{
		PredefineFetcherConfig: fetcherConfig,
	}
	return f, nil
}

func (p *PredefineFetcher) Fetch(groupname string) (hosts.Hosts, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	hosts, ok := p.Clusters[groupname]
	if !ok {
		return nil, fmt.Errorf("hosts for group `%s` are not specified", groupname)
	}
	return hosts, nil
}

func newHttpFetcher(context *Context, config map[string]interface{}) (HostFetcher, error) {
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
	resp, err := httpClient.Get(url)
	var body []byte
	if err != nil {
		log.Warningf("Unable to fetch hosts from %s: %s. Cache is used", url, err)
		body, err = s.Cache.Get(fetcherCacheNamespace, groupname)
		if err != nil {
			log.Errorf("Unable to read data from the cache: %s", err)
			return nil, err
		}
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Errorf("%s answered with %s. Cache is used", url, resp.Status)
			body, err = s.Cache.Get(fetcherCacheNamespace, groupname)
			if err != nil {
				return nil, err
			}
		} else {
			body, _ = ioutil.ReadAll(resp.Body)
			if put_err := s.Cache.Put(fetcherCacheNamespace, groupname, body); put_err != nil {
				log.Infof("Put error: %s", put_err)
			}
		}
	}

	// Body parsing
	fetchedHosts := make(hosts.Hosts)
	items := strings.TrimSuffix(string(body), "\n")
	for _, dcAndHost := range strings.Split(items, "\n") {
		temp := strings.Split(dcAndHost, s.Separator)
		if len(temp) != 2 {
			log.Infof("Wrong input string %s", temp)
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
