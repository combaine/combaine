package combainer

import (
	"fmt"
	"io/ioutil"
	"launchpad.net/goyaml"
	"net/http"
	"strings"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

const (
	CONFIGS_PARSING_PATH = "/etc/combaine/parsing/"
	COMBAINER_PATH       = "/etc/combaine/combaine.yaml"
)

type ParsingConfig struct {
	Groups           []string "groups"
	AggConfigs       []string "agg_configs"
	Metahost         string   "metahost"
	combainerMainCfg "Combainer"
}

const (
	CACHE_NAMESPACE = "combaine_hosts_cache"
)

type Cache interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
}

type cloudStorageCache struct {
	storage *cocaine.Service
}

var (
	cache Cache
)

func init() {
	var err error
	cache, err = NewCacher()
	if err != nil {
		panic(fmt.Sprintf("Unable to create Cacher %s", err))
	}
}

func NewCacher() (Cache, error) {
	s, err := cocaine.NewService("storage")
	if err != nil {
		return nil, err
	}
	return &cloudStorageCache{
		storage: s,
	}, nil
}

func (c *cloudStorageCache) Put(key string, value []byte) error {
	res, ok := <-c.storage.Call("write", CACHE_NAMESPACE, key, []byte(value))
	if !ok {
		return nil
	}
	return res.Err()
}

func (c *cloudStorageCache) Get(key string) ([]byte, error) {
	res := <-c.storage.Call("read", CACHE_NAMESPACE, key)

	if err := res.Err(); err != nil {
		return nil, err
	}

	var z []byte
	if err := res.Extract(&z); err != nil {
		return nil, err
	}
	return z, nil
}

// Fetch hosts by groupname from HTTP
func GetHosts(handle string, groupname string) (hosts []string, err error) {
	url := fmt.Sprintf("%s%s", handle, groupname)
	resp, err := http.Get(url)
	var body []byte
	if err != nil {
		LogErr("Unable to fetch hosts from %s: %s. Cache is used", url, err)
		body, err = cache.Get(groupname)
		if err != nil {
			return nil, err
		}
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			LogErr("%s answered with %s. Cache is used", url, resp.Status)
			body, err = cache.Get(groupname)
			if err != nil {
				return nil, err
			}
		} else {
			body, _ = ioutil.ReadAll(resp.Body)
			if put_err := cache.Put(groupname, body); put_err != nil {
				LogInfo("Put error: %s", put_err)
			}
		}
	}

	// Body parsing
	s := strings.TrimSuffix(string(body), "\n")
	for _, dcAndHost := range strings.Split(s, "\n") {
		if temp := strings.Split(dcAndHost, " "); len(temp) == 2 {
			hosts = append(hosts, temp[1])
		} else {
			LogInfo("Wrong input string %s", temp)
		}
	}
	if len(hosts) == 0 {
		return hosts, fmt.Errorf("No hosts")
	}
	return hosts, nil
}

// Return listing of configuration files
func getConfigs(path string) []string {
	var s []string
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".json") || strings.HasSuffix(f.Name(), ".yaml") {
			s = append(s, f.Name())
		}
	}
	return s
}

func getParsings() []string {
	return getConfigs(CONFIGS_PARSING_PATH)
}

// Parse config
func loadConfig(name string) (*ParsingConfig, error) {
	path := fmt.Sprintf("%s%s", CONFIGS_PARSING_PATH, name)
	LogInfo("Read %s", path)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Parse combaine.yaml
	var res ParsingConfig
	err = goyaml.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}
