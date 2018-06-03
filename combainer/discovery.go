package combainer

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/chttp"
	"github.com/combaine/combaine/common/hosts"
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
type FetcherLoader func(cache.Cache, map[string]interface{}) (HostFetcher, error)

var (
	fetchers = make(map[string]FetcherLoader)
)

// RegisterFetcherLoader register new fetcher loader function
func RegisterFetcherLoader(name string, f FetcherLoader) error {
	if _, ok := fetchers[name]; ok {
		return fmt.Errorf("HostFetcher `%s` is already registered", name)
	}
	fetchers[name] = f
	return nil
}

// LoadHostFetcher create, configure and return new hosts fetcher
func LoadHostFetcher(cache cache.Cache, config common.PluginConfig) (HostFetcher, error) {
	name, err := config.Type()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get type of HostFetcher")
	}

	if initializer, ok := fetchers[name]; ok {
		return initializer(cache, config)
	}
	return nil, fmt.Errorf("HostFetcher `%s` isn't registered", name)
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
// cache ignored because cache not need this
func newPredefineFetcher(_ cache.Cache, config map[string]interface{}) (HostFetcher, error) {
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
// or json configured by Format config
type SimpleFetcher struct {
	SimpleFetcherConfig
	Cache cache.Cache
}

// SimpleFetcherConfig contains parmeters from 'parsing' section of the combainer config
type SimpleFetcherConfig struct {
	Separator   string
	Format      string
	ReadTimeout int64
	Options     map[string]string
	BasicURL    string `mapstructure:"BasicUrl"`
}

// newHTTPFetcher return list of hosts fethed from http discovery service
func newHTTPFetcher(cache cache.Cache, config map[string]interface{}) (HostFetcher, error) {
	var fetcherConfig SimpleFetcherConfig
	if err := mapstructure.Decode(config, &fetcherConfig); err != nil {
		return nil, err
	}

	if fetcherConfig.Separator == "" {
		fetcherConfig.Separator = "\t"
	}
	if fetcherConfig.ReadTimeout <= 0 {
		fetcherConfig.ReadTimeout = 10
	}

	if fetcherConfig.Options == nil {
		fetcherConfig.Options = make(map[string]string)
	}
	if _, ok := fetcherConfig.Options["fqdn_key_name"]; !ok {
		fetcherConfig.Options["fqdn_key_name"] = "fqdn"
	}
	if _, ok := fetcherConfig.Options["dc_key_name"]; !ok {
		fetcherConfig.Options["dc_key_name"] = "root_datacenter_name"
	}

	f := &SimpleFetcher{
		SimpleFetcherConfig: fetcherConfig,
		Cache:               cache,
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.ReadTimeout)*time.Second)
	defer cancel()
	resp, err := chttp.Get(ctx, url)
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
	switch s.Format {
	case "json":
		return s.parseJSON(body)
	default:
		return s.parseTSV(body)
	}
}

func (s *SimpleFetcher) parseTSV(body []byte) (hosts.Hosts, error) {
	log := logrus.WithField("source", "SimpleFetcher.parseTSV")
	parsed := make(hosts.Hosts)
	items := strings.TrimSuffix(string(body), "\n")
	for _, dcAndHost := range strings.Split(items, "\n") {
		temp := strings.Split(dcAndHost, s.Separator)
		// expect index 0 - datacenter, 1 - fqdn
		if len(temp) != 2 {
			log.Errorf("Wrong input string %q", dcAndHost)
			continue
		}
		if temp[0] == "" {
			temp[0] = "NoDC"
		}
		dc, host := temp[0], temp[1]
		parsed[dc] = append(parsed[dc], host)
	}
	if len(parsed) == 0 {
		return parsed, common.ErrNoHosts
	}
	return parsed, nil
}

func (s *SimpleFetcher) parseJSON(body []byte) (hosts.Hosts, error) {
	log := logrus.WithField("source", "SimpleFetcher.parseJSON")

	var resp []map[string]string
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("Failed to parse json body: %s", err)
	}

	parsed := make(hosts.Hosts)
	var fqdn string
	var dc string
	var ok bool

	for _, dcAndHost := range resp {
		if fqdn, ok = dcAndHost[s.Options["fqdn_key_name"]]; !ok || fqdn == "" {
			log.Errorf("Wrong fqdn in host description '%s'", dcAndHost)
			continue
		}
		if dc, ok = dcAndHost[s.Options["dc_key_name"]]; !ok || dc == "" {
			dc = "NoDC"
		}
		parsed[dc] = append(parsed[dc], fqdn)
	}
	if len(parsed) == 0 {
		return parsed, common.ErrNoHosts
	}
	return parsed, nil
}

// RTCFetcher recive hosts from RTC groups
type RTCFetcher struct {
	RTCFetcherConfig
	Cache cache.Cache
}

// RTCFetcherConfig ...
type RTCFetcherConfig struct {
	Separator   string
	Format      string
	ReadTimeout int64
	Options     map[string]string
	BasicURL    string `mapstructure:"BasicUrl"`
}
