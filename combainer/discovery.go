package combainer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/hosts"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/mitchellh/mapstructure"
)

func init() {
	if err := RegisterFetcherLoader("http", newHTTPFetcher); err != nil {
		panic(err)
	}
	if err := RegisterFetcherLoader("predefine", newPredefineFetcher); err != nil {
		panic(err)
	}
}

const fetcherCacheNamespace = "simpleFetcherCacheNamespace"

// FetcherLoader is type of function is responsible for loading fetchers
type FetcherLoader func(*Context, map[string]interface{}) (HostFetcher, error)

var (
	fetchers   = make(map[string]FetcherLoader)
	httpClient = httpclient.NewClientWithTimeout(1*time.Second, 3*time.Second)
)

// RegisterFetcherLoader register new fetcher loader function
func RegisterFetcherLoader(name string, f FetcherLoader) error {
	_, ok := fetchers[name]
	if ok {
		return fmt.Errorf("HostFetcher `%s` is already registered", name)
	}

	fetchers[name] = f
	return nil
}

// LoadHostFetcher create, configure and return new hosts fetcher
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

// HostFetcher interface
type HostFetcher interface {
	Fetch(group string) (hosts.Hosts, error)
}

// PredefineFetcher is map[string /*datacenter name*/][]string /*list of hosts*/
type PredefineFetcher struct {
	mutex sync.Mutex
	PredefineFetcherConfig
}

// PredefineFetcherConfig filled from combainer config
type PredefineFetcherConfig struct {
	Clusters map[string]hosts.Hosts
}

// newPredefineFetcher return list of hosts defined
// in user or server level combainer's config,
// context parameter ignored because cache not need this
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

// Fetch return hosts list from PredefineFetcher or error
func (p *PredefineFetcher) Fetch(groupname string) (hosts.Hosts, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	hosts, ok := p.Clusters[groupname]
	if !ok {
		return nil, fmt.Errorf("hosts for group `%s` are not specified", groupname)
	}
	return hosts, nil
}

// SimpleFetcher recive plain text with tab separated fields
// it expect format `fqdn\tdatacenter`
type SimpleFetcher struct {
	SimpleFetcherConfig
	*Context
}

// SimpleFetcherConfig contains parmeters from 'parsing' section of the combainer config
type SimpleFetcherConfig struct {
	Separator string
	BasicURL  string `mapstructure:"BasicUrl"`
}

// newHTTPFetcher return list of hosts fethed from http discovery service
// context used for provide combainer Cache
func newHTTPFetcher(context *Context, config map[string]interface{}) (HostFetcher, error) {
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

// Fetch resolve the group name in the list of hosts
func (s *SimpleFetcher) Fetch(groupname string) (hosts.Hosts, error) {
	log := logrus.WithField("source", "SimpleFetcher")
	if !strings.Contains(s.BasicURL, `%s`) {
		return nil, common.ErrMissingFormatSpecifier
	}
	url := fmt.Sprintf(s.BasicURL, groupname)
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
			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Errorf("Failed to read response: %s", err)
				body, err = s.Cache.Get(fetcherCacheNamespace, groupname)
				if err != nil {
					log.Errorf("Unable to read data from the cache: %s", err)
					return nil, err
				}
				log.Errorf("%s answered with %s, but read response failed. Cache is used", url, resp.Status)
			} else {
				log.Debugf("Put in cache %s: %q", groupname, body)
				if putErr := s.Cache.Put(fetcherCacheNamespace, groupname, body); putErr != nil {
					log.Warnf("Put error: %s", putErr)
				}
			}
		}
	}

	// Body parsing
	fetchedHosts := make(hosts.Hosts)
	items := strings.TrimSuffix(string(body), "\n")
	for _, dcAndHost := range strings.Split(items, "\n") {
		temp := strings.Split(dcAndHost, s.Separator)
		// expect index 0 - datacenter, 1 - fqdn
		if len(temp) != 2 || temp[0] == "" {
			log.Errorf("Wrong input string %q", temp)
			continue
		}
		dc, host := temp[0], temp[1]
		fetchedHosts[dc] = append(fetchedHosts[dc], host)
	}
	if len(fetchedHosts) == 0 {
		return fetchedHosts, common.ErrNoHosts
	}
	return fetchedHosts, nil
}
