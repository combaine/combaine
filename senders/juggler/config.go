package juggler

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/combaine/combaine/common/configs"
	yaml "gopkg.in/yaml.v2"
)

const (
	DEFAULT_CONFIG_PATH  = "/etc/combaine/juggler.yaml"
	DEFAULT_PLUGIN_DIR   = "/usr/lib/yandex/combaine/juggler"
	DEFAULT_CHECK_STATUS = "OK"
)

type Conditions struct {
	OK   []string `codec:"OK"`
	INFO []string `codec:"INFO"`
	WARN []string `codec:"WARN"`
	CRIT []string `codec:"CRIT"`
}

type JugglerConfig struct {
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
	Flap               *JugglerFlapConfig            `codec:"flap"`
	ChecksOptions      map[string]*JugglerFlapConfig `codec:"checks_options"`
	JPluginConfig      configs.PluginConfig          `codec:"config"`
	JHosts             []string                      `codec:"juggler_hosts"`
	JFrontend          []string                      `codec:"juggler_frontend"`
	Conditions
}

type jugglerSenderConfig struct {
	PluginsDir string   `yaml:"plugins_dir"`
	Hosts      []string `yaml:"juggler_hosts"`
	Frontend   []string `yaml:"juggler_frontend"`
}

// GetJugglerSenderConfig read yaml file with two arrays of hosts
// if juggler_frontend not defined, use juggler_hosts as frontend
func GetJugglerSenderConfig() (conf jugglerSenderConfig, err error) {
	var path string = os.Getenv("JUGGLER_CONFIG")
	if len(path) == 0 {
		path = DEFAULT_CONFIG_PATH
	}

	rawConfig, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	err = yaml.Unmarshal(rawConfig, &conf)
	if conf.Frontend == nil {
		conf.Frontend = conf.Hosts
	}
	if conf.PluginsDir == "" {
		conf.PluginsDir = DEFAULT_PLUGIN_DIR
	}
	return
}

func DefaultJugglerConfig() *JugglerConfig {
	return &JugglerConfig{
		PluginsDir:         "/etc/combaine/juggler/plugins",
		Plugin:             "",
		DefaultCheckStatus: DEFAULT_CHECK_STATUS,
		Host:               "",
		Methods:            []string{},
		Aggregator:         "",
		CheckName:          "",
		Description:        "",
		Tags:               []string{"combaine"},
		AggregatorKWargs:   json.RawMessage{},
		Flap:               nil,
		ChecksOptions:      make(map[string]*JugglerFlapConfig, 0),
		JPluginConfig:      configs.PluginConfig{},
		JHosts:             []string{},
		JFrontend:          []string{},
		Conditions:         Conditions{},
	}
}
