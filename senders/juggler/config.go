package juggler

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/combaine/combaine/common/configs"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultConfigPath  = "/etc/combaine/juggler.yaml"
	defaultPluginsDir  = "/usr/lib/yandex/combaine/juggler"
	defaultCheckStatus = "OK"
)

type conditions struct {
	OK   []string `codec:"OK"`
	INFO []string `codec:"INFO"`
	WARN []string `codec:"WARN"`
	CRIT []string `codec:"CRIT"`
}

// Config contains config section from combainer's aggregations section
// also it include defaultConfigPath (or user specified) yaml config
type Config struct {
	PluginsDir         string                        `codec:"plugins_dir"`
	Plugin             string                        `codec:"plugin"`
	DefaultCheckStatus string                        `codec:"default_status"`
	Host               string                        `codec:"Host"`
	Methods            []string                      `codec:"Methods"`
	Aggregator         string                        `codec:"Aggregator"`
	CheckName          string                        `codec:"checkname"`
	Description        string                        `codec:"description"`
	Tags               []string                      `codec:"tags"`
	AggregatorKWargs   json.RawMessage               `codec:"aggregator_kwargs"`
	Flap               *jugglerFlapConfig            `codec:"flap"`
	ChecksOptions      map[string]*jugglerFlapConfig `codec:"checks_options"`
	JPluginConfig      configs.PluginConfig          `codec:"config"`
	JHosts             []string                      `codec:"juggler_hosts"`
	JFrontend          []string                      `codec:"juggler_frontend"`
	conditions
}

// SenderConfig contains configuration loaded from combaine's config file
// placed in defaultConfigPath
type SenderConfig struct {
	PluginsDir string   `yaml:"plugins_dir"`
	Hosts      []string `yaml:"juggler_hosts"`
	Frontend   []string `yaml:"juggler_frontend"`
}

// GetJugglerSenderConfig read yaml file with two arrays of hosts
// if juggler_frontend not defined, use juggler_hosts as frontend
func GetJugglerSenderConfig() (conf SenderConfig, err error) {
	var path = os.Getenv("JUGGLER_CONFIG")
	if len(path) == 0 {
		path = defaultConfigPath
	}

	rawConfig, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	err = yaml.Unmarshal(rawConfig, &conf)
	if len(conf.Frontend) == 0 {
		conf.Frontend = conf.Hosts
	}
	if conf.PluginsDir == "" {
		conf.PluginsDir = defaultPluginsDir
	}
	return conf, nil
}

// DefaultConfig build default config for sender, it has sanity defaults
func DefaultConfig() *Config {
	return &Config{
		PluginsDir:         "/etc/combaine/juggler/plugins",
		Plugin:             "",
		DefaultCheckStatus: defaultCheckStatus,
		Host:               "",
		Methods:            []string{},
		Aggregator:         "",
		CheckName:          "",
		Description:        "",
		Tags:               []string{"combaine"},
		AggregatorKWargs:   json.RawMessage{},
		Flap:               nil,
		ChecksOptions:      make(map[string]*jugglerFlapConfig, 0),
		JPluginConfig:      configs.PluginConfig{},
		JHosts:             []string{},
		JFrontend:          []string{},
		conditions:         conditions{},
	}
}
