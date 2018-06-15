package juggler

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/combaine/combaine/repository"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultConfigPath = "/etc/combaine/juggler.yaml"
	defaultPlugin     = "simple"
	defaultPluginsDir = "/usr/lib/yandex/combaine/juggler"
	defaultBatchSize  = 50 // send 50 events in one batch
)

// default send timout
var hostname, _ = os.Hostname()

// DefaultTimeout read timeout
var DefaultTimeout = 5 * time.Second

// Config contains config section from combainer's aggregations section
// also it include default sender config (or user specified) yaml config
type Config struct {
	PluginsDir       string                       `codec:"plugins_dir"`
	Plugin           string                       `codec:"plugin"`
	Host             string                       `codec:"Host"`
	Method           string                       `codec:"Method"`
	Methods          []string                     `codec:"Methods"`
	Aggregator       string                       `codec:"Aggregator"`
	AggregatorKWArgs aggKWArgs                    `codec:"aggregator_kwargs"`
	TTL              int                          `codec:"ttl"`
	CheckName        string                       `codec:"checkname"`
	Description      string                       `codec:"description"`
	Tags             []string                     `codec:"tags"`
	Flap             *jugglerFlapConfig           `codec:"flap"`
	Variables        map[string]string            `codec:"variables"`
	ChecksOptions    map[string]jugglerFlapConfig `codec:"checks_options"`
	JPluginConfig    repository.PluginConfig      `codec:"config"`
	JHosts           []string                     `codec:"juggler_hosts"`
	JFrontend        []string                     `codec:"juggler_frontend"`
	OK               []string                     `codec:"OK"`
	INFO             []string                     `codec:"INFO"`
	WARN             []string                     `codec:"WARN"`
	CRIT             []string                     `codec:"CRIT"`
	BatchSize        int                          `codec:"batch_size"`
	BatchEndpoint    string                       `codec:"batch_endpoint"`
	Namespace        string                       `codec:"namespace"`
	Token            string                       `codec:"-"` // do not pass token
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

// StringifyAggregatorLimits check all AggregatorLimits values
// and convert it to sting
func StringifyAggregatorLimits(limits []map[string]interface{}) {
	for _, v := range limits {
		for k, iv := range v {
			if byteVal, ok := iv.([]byte); ok {
				// json encode []bytes in base64, but there string expected
				iv = string(byteVal)
				v[k] = iv
			}
		}
	}
}

// EnsureDefaultTag add default tag "combaine" if it not present in tags
func EnsureDefaultTag(jtags []string) []string {
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
	return &sConf, nil
}

// UpdateTaskConfig update current task config,
// set default settings if it need
func UpdateTaskConfig(taskConf *Config, conf *SenderConfig) error {
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
	if conf.Token != "" && conf.Token != "no-token" {
		taskConf.Token = conf.Token
	}
	return nil
}

// DefaultConfig build default config for sender, it has sanity defaults
func DefaultConfig() *Config {
	return &Config{
		PluginsDir:       "/etc/combaine/juggler/plugins",
		Plugin:           "",
		Host:             "",
		Methods:          []string{},
		Aggregator:       "",
		AggregatorKWArgs: aggKWArgs{},
		CheckName:        "",
		Description:      "",
		Tags:             []string{"combaine"},
		Flap:             nil,
		ChecksOptions:    make(map[string]jugglerFlapConfig, 0),
		JPluginConfig:    repository.PluginConfig{},
		JHosts:           []string{},
		JFrontend:        []string{},
		BatchEndpoint:    "",
		OK:               []string{},
		INFO:             []string{},
		WARN:             []string{},
		CRIT:             []string{},
	}
}
