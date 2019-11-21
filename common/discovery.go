package common

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/samuel/go-zookeeper/zk"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/chttp"
	"github.com/combaine/combaine/common/hosts"
	"github.com/combaine/combaine/repository"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	if err := RegisterFetcherLoader("http", newHTTPFetcher); err != nil {
		panic(err)
	}
	if err := RegisterFetcherLoader("predefine", newPredefineFetcher); err != nil {
		panic(err)
	}
	if err := RegisterFetcherLoader("rtc", newRTCFetcher); err != nil {
		panic(err)
	}
	if err := RegisterFetcherLoader("zk", newZKFetcher); err != nil {
		panic(err)
	}
	if err := RegisterFetcherLoader("qloud", newQDNSFetcher); err != nil {
		panic(err)
	}
}

// FetcherLoader is type of function is responsible for loading fetchers
type FetcherLoader func(repository.PluginConfig) (HostFetcher, error)

var fetchers = make(map[string]FetcherLoader)

// RegisterFetcherLoader register new fetcher loader function
func RegisterFetcherLoader(name string, f FetcherLoader) error {
	if _, ok := fetchers[name]; ok {
		return fmt.Errorf("HostFetcher `%s` is already registered", name)
	}
	fetchers[name] = f
	return nil
}

// LoadHostFetcher create, configure and return new hosts fetcher
func LoadHostFetcher(config repository.PluginConfig) (HostFetcher, error) {
	name, err := config.Type()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get type of HostFetcher")
	}

	if initializer, ok := fetchers[name]; ok {
		return initializer(config)
	}
	return nil, fmt.Errorf("HostFetcher `%s` isn't registered", name)
}

// LoadHostFetcherWithCache create, configure and return new hosts fetcher
func LoadHostFetcherWithCache(config repository.PluginConfig, cache *cache.TTLCache) (HostFetcher, error) {
	name, err := config.Type()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get type of HostFetcher")
	}

	if initializer, ok := fetchers[name]; ok {
		f, err := initializer(config)
		if err != nil {
			return nil, err
		}
		f.setCache(cache)
		return f, nil
	}
	return nil, fmt.Errorf("HostFetcher `%s` isn't registered", name)
}

// HostFetcher interface
type HostFetcher interface {
	Fetch(group string) (hosts.Hosts, error)
	setCache(cache *cache.TTLCache)
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
func newPredefineFetcher(config repository.PluginConfig) (HostFetcher, error) {
	var fetcherConfig PredefineFetcherConfig
	if err := mapstructure.Decode(config, &fetcherConfig); err != nil {
		return nil, err
	}

	f := &PredefineFetcher{
		PredefineFetcherConfig: fetcherConfig,
	}
	return f, nil
}
func (p *PredefineFetcher) setCache(_ *cache.TTLCache) {
	return
}

// Fetch return hosts list from PredefineFetcher or error
func (p *PredefineFetcher) Fetch(groupname string) (hosts.Hosts, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	hostList, ok := p.Clusters[groupname]
	if !ok {
		return nil, fmt.Errorf("hosts for group `%s` are not specified", groupname)
	}
	return hostList, nil
}

// SimpleFetcher recive plain text with tab separated fields
// it expect format `fqdn\tdatacenter`
// or json configured by Format config
type SimpleFetcher struct {
	cache       *cache.TTLCache
	Separator   string
	Format      string
	ReadTimeout int64
	Options     map[string]string
	BasicURL    string `mapstructure:"BasicUrl"`
}

// newHTTPFetcher return list of hosts fethed from http discovery service
func newHTTPFetcher(config repository.PluginConfig) (HostFetcher, error) {
	var fetcher SimpleFetcher
	if err := mapstructure.Decode(config, &fetcher); err != nil {
		return nil, err
	}

	if fetcher.Separator == "" {
		fetcher.Separator = "\t"
	}
	if fetcher.ReadTimeout <= 0 {
		fetcher.ReadTimeout = 10
	}

	if fetcher.Options == nil {
		fetcher.Options = make(map[string]string)
	}
	if _, ok := fetcher.Options["fqdn_key_name"]; !ok {
		fetcher.Options["fqdn_key_name"] = "fqdn"
	}
	if _, ok := fetcher.Options["dc_key_name"]; !ok {
		fetcher.Options["dc_key_name"] = "root_datacenter_name"
	}

	return &fetcher, nil
}

// Fetch resolve the group name in the list of hosts
func (s *SimpleFetcher) Fetch(groupname string) (hosts.Hosts, error) {
	log := logrus.WithField("source", "SimpleFetcher")
	if !strings.Contains(s.BasicURL, `%s`) {
		return nil, ErrMissingFormatSpecifier
	}
	url := fmt.Sprintf(s.BasicURL, groupname)

	fetcher := func() ([]byte, error) {
		ctx, cancel := context.WithTimeout(
			context.Background(), time.Duration(s.ReadTimeout)*time.Second,
		)
		defer cancel()
		resp, err := chttp.Get(ctx, url)
		var body []byte
		if err != nil {
			log.Errorf("Unable to fetch hosts from %s: %s", url, err)
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			err = errors.Errorf("%s answered with %s", url, resp.Status)
			log.Error(err)
			return nil, err
		}
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("Failed to read response: %s", err)
			return nil, err
		}
		return body, nil
	}
	var body []byte
	var err error
	if s.cache != nil {
		body, err = s.cache.GetBytes(groupname, url, fetcher)
	} else {
		body, err = fetcher()
	}
	if err != nil {
		return nil, err
	}

	// Body parsing
	switch s.Format {
	case "json":
		return s.parseJSON(body)
	default:
		return s.parseTSV(body)
	}
}
func (s *SimpleFetcher) setCache(c *cache.TTLCache) {
	s.cache = c
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
		return parsed, ErrNoHosts
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
		return parsed, ErrNoHosts
	}
	return parsed, nil
}

// RTCFetcher recive hosts from RTC groups
type RTCFetcher struct {
	cache       *cache.TTLCache
	ReadTimeout int64
	Geo         []string `mapstructure:"geo"`
	BasicURL    string   `mapstructure:"BasicUrl"`
	Delimiter   string   `mapstructure:"Delimiter"`
}

// newRTCFetcher return list of hosts fethed from http discovery service
func newRTCFetcher(config repository.PluginConfig) (HostFetcher, error) {
	var fetcher RTCFetcher
	if err := mapstructure.Decode(config, &fetcher); err != nil {
		return nil, err
	}

	if fetcher.Delimiter == "" {
		fetcher.Delimiter = "_"
	}

	if fetcher.ReadTimeout <= 0 {
		fetcher.ReadTimeout = 10
	}
	return &fetcher, nil
}

// Fetch resolve the group name in the list of hosts
func (s *RTCFetcher) Fetch(groupname string) (hosts.Hosts, error) {
	log := logrus.WithField("source", "RTCFetcher")
	if !strings.Contains(s.BasicURL, `%s`) {
		return nil, ErrMissingFormatSpecifier
	}

	var response = make(hosts.Hosts)
	for _, geo := range s.Geo {
		suffix := geo
		if suffix != "" {
			suffix = s.Delimiter + suffix
		}

		urlGeo := fmt.Sprintf(s.BasicURL, groupname+suffix)

		fetcher := func() ([]byte, error) {
			ctx, cancel := context.WithTimeout(
				context.Background(), time.Duration(s.ReadTimeout)*time.Second,
			)
			defer cancel()
			resp, err := chttp.Get(ctx, urlGeo)
			var body []byte
			if err != nil {
				log.Errorf("Unable to fetch hosts from %s: %s", urlGeo, err)
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				err = errors.Errorf("%s answered with %s", urlGeo, resp.Status)
				log.Error(err)
				return nil, err
			}
			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Errorf("Failed to read response: %s", err)
				return nil, err
			}
			return body, nil
		}
		var body []byte
		var err error
		if s.cache != nil {
			body, err = s.cache.GetBytes(groupname, urlGeo, fetcher)
		} else {
			body, err = fetcher()
		}
		if err != nil {
			log.Errorf("Cache.Get failed for: %s", urlGeo)
			continue
		}
		hostnames, err := s.parseJSON(body)
		if err != nil {
			log.Errorf("Failed to parse response: %s", err)
		}
		if len(hostnames) != 0 {
			response[geo] = hostnames
		}
	}
	if len(response) == 0 {
		return response, ErrNoHosts
	}
	return response, nil
}
func (s *RTCFetcher) setCache(c *cache.TTLCache) {
	s.cache = c
}

type rtcResponse struct {
	Items []rtcItem `json:"result"`
}
type rtcItem struct {
	Hostname string `json:"container_hostname"`
}

func (s *RTCFetcher) parseJSON(body []byte) ([]string, error) {
	var resp rtcResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	hostList := make([]string, len(resp.Items))
	for idx, item := range resp.Items {
		hostList[idx] = item.Hostname
	}
	return hostList, nil
}

// ZKFetcher recive hosts from ZK dir
type ZKFetcher struct {
	cache     *cache.TTLCache
	Servers   []string `mapstructure:"servers"`
	StripPort bool     `mapstructure:"strip_port"`
}

// newZKFetcher return list of hosts fetched from zk discovery service
func newZKFetcher(config repository.PluginConfig) (HostFetcher, error) {
	var fetcher ZKFetcher
	if err := mapstructure.Decode(config, &fetcher); err != nil {
		return nil, err
	}
	return &fetcher, nil
}

// Fetch hosts from zk node
func (s *ZKFetcher) Fetch(path string) (hosts.Hosts, error) {
	log := logrus.WithField("source", "ZKFetcher")

	var response = make(hosts.Hosts)

	url := "zk://" + strings.Join(s.Servers, ",") + "/" + path
	fetcher := func() ([]string, error) {

		conn, _, err := zk.Connect(s.Servers, time.Second*10, zk.WithLogger(log))
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		list, _, err := conn.Children(path)
		if err != nil {
			return nil, err
		}

		return list, nil
	}

	var list []string
	var err error
	if s.cache != nil {
		list, err = s.cache.GetStrings(path, url, fetcher)
	} else {
		list, err = fetcher()
	}
	if err != nil {
		return nil, err
	}

	if s.StripPort {
		for idx, item := range list {
			delimIdx := strings.LastIndex(item, ":")
			if delimIdx > -1 {
				list[idx] = item[:delimIdx]
			}
		}
	}

	response["unk"] = list
	return response, nil
}

func (s *ZKFetcher) setCache(c *cache.TTLCache) {
	s.cache = c
}

// QDNSFetcher recive hosts from dns discovery
type QDNSFetcher struct {
	cache        *cache.TTLCache
	Resolver     string
	QueryTimeout int64
	timeout      time.Duration
}

// newQDNSFetcher return dns discovery client
func newQDNSFetcher(config repository.PluginConfig) (HostFetcher, error) {
	var fetcher QDNSFetcher
	if err := mapstructure.Decode(config, &fetcher); err != nil {
		return nil, err
	}
	if fetcher.Resolver == "" {
		fetcher.Resolver = "[::1]:53"
	}
	if fetcher.QueryTimeout == 0 {
		fetcher.QueryTimeout = 10
	}
	fetcher.timeout = time.Duration(fetcher.QueryTimeout) * time.Second
	return &fetcher, nil
}

func dcIndex(c rune) bool {
	if c < 0x61 {
		c = c + 0x20 // make lowercase
	}
	return c < 'a' || c > 'z'
}

// Fetch hosts from dns discovery service
func (d *QDNSFetcher) Fetch(entity string) (hosts.Hosts, error) {
	log := logrus.WithField("source", "QDNSFetcher")

	if !strings.HasSuffix(entity, ".") {
		entity += "."
	}

	fetcher := func() (map[string][]string, error) {
		m := new(dns.Msg)
		m.SetQuestion(entity, dns.TypeSRV)
		var in *dns.Msg
		var err error

		client := dns.Client{Net: "tcp", Timeout: d.timeout}
		in, _, err = client.Exchange(m, d.Resolver)
		if err != nil {
			log.Errorf("%s %s", entity, err)
			return nil, err
		}
		var targets []string
		for _, r := range in.Answer {
			if t, ok := r.(*dns.SRV); ok {
				targets = append(targets, t.Target)
			}
		}
		discovered := make(map[string][]string)
		for _, t := range targets {
			m.SetQuestion("_host_."+t, dns.TypeSRV)
			in, err := dns.Exchange(m, d.Resolver)
			if err != nil {
				log.Errorf("%s %s", entity, err)
				continue
			}
			if rt, ok := in.Answer[0].(*dns.SRV); ok {
				dcIdx := strings.IndexFunc(rt.Target, dcIndex)
				if dcIdx > 0 {
					dc := rt.Target[:dcIdx]
					discovered[dc] = append(discovered[dc], strings.TrimRight(t, "."))
				} else {
					discovered["NoDC"] = append(discovered["NoDC"], strings.TrimRight(t, "."))
				}
			}
		}
		if len(discovered) == 0 {
			return nil, ErrNoHosts
		}
		return discovered, nil
	}

	var response hosts.Hosts
	var err error
	if d.cache != nil {
		response, err = d.cache.GetMapStringStrings(entity, entity, fetcher)
	} else {
		response, err = fetcher()
	}
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (d *QDNSFetcher) setCache(c *cache.TTLCache) {
	d.cache = c
}
