package juggler

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/combaine/combaine/common"

	yaml "gopkg.in/yaml.v2"
)

const (
	defaultConfigPath = "/etc/combaine/juggler.yaml"
	defaultPluginsDir = "/usr/lib/yandex/combaine/juggler"
)

// Config contains config section from combainer's aggregations section
// also it include defaultConfigPath (or user specified) yaml config
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
	JPluginConfig    common.PluginConfig          `codec:"config"`
	JHosts           []string                     `codec:"juggler_hosts"`
	JFrontend        []string                     `codec:"juggler_frontend"`
	OK               []string                     `codec:"OK"`
	INFO             []string                     `codec:"INFO"`
	WARN             []string                     `codec:"WARN"`
	CRIT             []string                     `codec:"CRIT"`
}

// SenderConfig contains configuration loaded from combaine's config file
// placed in defaultConfigPath
type SenderConfig struct {
	CacheTTL           time.Duration `yaml:"cache_ttl"`
	CacheCleanInterval time.Duration `yaml:"cache_clean_interval"`
	PluginsDir         string        `yaml:"plugins_dir"`
	Hosts              []string      `yaml:"juggler_hosts"`
	Frontend           []string      `yaml:"juggler_frontend"`
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

// GetSenderConfig read yaml file with two arrays of hosts
// if juggler_frontend not defined, use juggler_hosts as frontend
func GetSenderConfig() (conf SenderConfig, err error) {
	var path = os.Getenv("JUGGLER_CONFIG")
	if len(path) == 0 {
		path = defaultConfigPath
	}

	rawConfig, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	err = yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		return
	}
	if len(conf.Frontend) == 0 {
		conf.Frontend = conf.Hosts
	}
	if conf.PluginsDir == "" {
		conf.PluginsDir = defaultPluginsDir
	}

	if conf.CacheTTL == 0 {
		conf.CacheTTL = 60
	}
	if conf.CacheCleanInterval == 0 {
		conf.CacheCleanInterval = conf.CacheTTL * 5
	}
	conf.CacheTTL *= time.Second
	conf.CacheCleanInterval *= time.Second
	return conf, nil
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
		JPluginConfig:    common.PluginConfig{},
		JHosts:           []string{},
		JFrontend:        []string{},
		OK:               []string{},
		INFO:             []string{},
		WARN:             []string{},
		CRIT:             []string{},
	}
}
