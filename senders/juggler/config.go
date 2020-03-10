package juggler

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultConfigPath   = "/etc/combaine/juggler.yaml"
	defaultPlugin       = "simple"
	defaultPluginsDir   = "/usr/lib/combaine/juggler"
	defaultBatchSize    = 50 // send 50 events in one batch
	defaultDatabaseName = "combaine"
)

// Config contains config section from combainer's aggregations section
// also it include default sender config (or user specified) yaml config
type Config struct {
	PluginsDir       string                       `msgpack:"plugins_dir"`
	Plugin           string                       `msgpack:"plugin"`
	Host             string                       `msgpack:"Host"`
	Method           string                       `msgpack:"Method"`
	Aggregator       string                       `msgpack:"Aggregator"`
	AggregatorKWArgs repository.PluginConfig      `msgpack:"aggregator_kwargs"`
	Notifications    []repository.PluginConfig    `msgpack:"notifications"`
	TTL              int                          `msgpack:"ttl"`
	CheckName        string                       `msgpack:"checkname"`
	Description      string                       `msgpack:"description"`
	Tags             []string                     `msgpack:"tags"`
	Flaps            *jugglerFlapConfig           `msgpack:"flaps"`
	Variables        map[string]string            `msgpack:"variables"`
	FlapsByChecks    map[string]jugglerFlapConfig `msgpack:"flaps_by_checks"`
	JPluginConfig    repository.PluginConfig      `msgpack:"config"`
	JHosts           []string                     `msgpack:"juggler_hosts"`
	JFrontend        []string                     `msgpack:"juggler_frontend"`
	OK               []string                     `msgpack:"OK"`
	INFO             []string                     `msgpack:"INFO"`
	WARN             []string                     `msgpack:"WARN"`
	CRIT             []string                     `msgpack:"CRIT"`
	BatchSize        int                          `msgpack:"batch_size"`
	BatchEndpoint    string                       `msgpack:"batch_endpoint"`
	Namespace        string                       `msgpack:"namespace"`
	Token            string                       `msgpack:"-"` // do not pass token
}

// SenderConfig contains configuration loaded from combaine's config file
// placed in sender config
type SenderConfig struct {
	CacheTTL           time.Duration           `yaml:"cache_ttl"`
	CacheCleanInterval time.Duration           `yaml:"cache_clean_interval"`
	PluginsDir         string                  `yaml:"plugins_dir"`
	Hosts              []string                `yaml:"juggler_hosts"`
	Frontend           []string                `yaml:"juggler_frontend"`
	BatchSize          int                     `yaml:"batch_size"`
	BatchEndpoint      string                  `yaml:"batch_endpoint"`
	Token              string                  `yaml:"token"`
	Store              pluginEventsStoreConfig `yaml:"store"`
}

// DefaultConfig build default config for sender, it has sanity defaults
func DefaultConfig() *Config {
	return &Config{
		PluginsDir:       "/etc/combaine/juggler/plugins",
		AggregatorKWArgs: make(map[string]interface{}),
		Tags:             []string{"combaine"},
		Flaps:            nil,
		FlapsByChecks:    make(map[string]jugglerFlapConfig, 0),
		JPluginConfig:    repository.PluginConfig{},
	}
}

func ensureDefaultTag(jtags []string) []string {
	for _, t := range jtags {
		if t == "combaine" {
			return jtags
		}
	}
	return append(jtags, "combaine")
}

func getConfigPath() string {
	path := os.Getenv("JUGGLER_CONFIG")
	if len(path) == 0 {
		path = defaultConfigPath
	}
	return path
}

// GetConfigDir where senders config placed
func GetConfigDir() string {
	return filepath.Dir(getConfigPath())
}

// GetSenderConfig read global sender config
// usual from /etc/combaine/juggler.yaml
func GetSenderConfig() (*SenderConfig, error) {
	path := getConfigPath()

	rawConfig, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var sConf SenderConfig
	err = yaml.Unmarshal(rawConfig, &sConf)
	if err != nil {
		return nil, err
	}
	if sConf.BatchSize == 0 {
		sConf.BatchSize = defaultBatchSize
	}
	if sConf.PluginsDir == "" {
		sConf.PluginsDir = defaultPluginsDir
	}
	if sConf.CacheTTL == 0 {
		sConf.CacheTTL = 60
	}
	if sConf.CacheCleanInterval == 0 {
		sConf.CacheCleanInterval = sConf.CacheTTL * 5
	}
	sConf.CacheTTL *= time.Second
	sConf.CacheCleanInterval *= time.Second
	// load store configs
	if _, err := sConf.Store.Fetcher.Type(); err == nil {
		fetcher, err := common.LoadHostFetcher(sConf.Store.Fetcher)
		if err != nil {
			return nil, err
		}
		hosts, err := fetcher.Fetch(sConf.Store.Cluster)
		if err != nil {
			return nil, err
		}
		sConf.Store.Cluster = strings.Join(hosts.AllHosts(), ",")

	}
	if sConf.Store.Database == "" {
		sConf.Store.Database = defaultDatabaseName
	}
	return &sConf, nil
}

// UpdateTaskConfig update current task config,
// set default settings if it need
func UpdateTaskConfig(taskConf *Config, conf *SenderConfig) error {

	taskConf.Tags = ensureDefaultTag(taskConf.Tags)

	if conf.Token != "" && conf.Token != "no-token" {
		taskConf.Token = conf.Token
	}

	if len(taskConf.JHosts) == 0 {
		if len(conf.Hosts) == 0 {
			return errors.New("juggler hosts not defined")
		}
		// if jhosts not in PluginConfig override both jhosts and jfrontend
		taskConf.JHosts = conf.Hosts
		taskConf.JFrontend = conf.Frontend
	}
	if len(taskConf.JFrontend) == 0 {
		taskConf.JFrontend = taskConf.JHosts // jhost is by default used as jfrontend
	}
	if taskConf.PluginsDir == "" {
		taskConf.PluginsDir = conf.PluginsDir
	}
	if taskConf.Plugin == "" {
		taskConf.Plugin = defaultPlugin
	}
	if taskConf.BatchSize == 0 {
		taskConf.BatchSize = conf.BatchSize
	}
	if taskConf.BatchEndpoint == "" {
		taskConf.BatchEndpoint = conf.BatchEndpoint
	}
	if taskConf.Aggregator == "" {
		taskConf.Aggregator = "timed_more_than_limit_is_problem" // add default
	}

	if taskConf.Method == "" && len(taskConf.Notifications) == 0 {
		taskConf.Method = "GOLEM" // add default
	}
	return nil
}
