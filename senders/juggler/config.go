package juggler

import (
	"io/ioutil"
	"os"

	"github.com/combaine/combaine/common/configs"
	yaml "gopkg.in/yaml.v2"
)

type Conditions struct {
	OK   []string `codec:"OK"`
	INFO []string `codec:"INFO"`
	WARN []string `codec:"WARN"`
	CRIT []string `codec:"CRIT"`
}

type JugglerConfig struct {
	PluginsDir         string                  `codec:"PluginsDir"`
	Plugin             string                  `codec:"Plugin"`
	DefaultCheckStatus string                  `codec:"DefaultCheckStatus"`
	Host               string                  `codec:"Host"`
	Methods            []string                `codec:"Methods"`
	Aggregator         string                  `codec:"Aggregator"`
	CheckName          string                  `codec:"checkname"`
	Description        string                  `codec:"description"`
	AggregatorKWargs   JugglerAggregatorKWArgs `codec:"aggregator_kwargs"`
	Flap               JugglerFlapConfig       `codec:"flap"`
	JPluginConfig      configs.PluginConfig    `codec:"config"`
	JHosts             []string                `codec:"juggler_hosts"`
	JFrontend          []string                `codec:"juggler_frontend"`
	Conditions
}

type jugglerSenderConf struct {
	PluginsDir string   `yaml:"PluginsDir"`
	Hosts      []string `yaml:"juggler_hosts"`
	Frontend   []string `yaml:"juggler_frontend"`
}

// GetJugglerConfig read yaml file with two arrays of hosts
// if juggler_frontend not defined, use juggler_hosts as frontend
func GetJugglerConfig() (conf jugglerSenderConf, err error) {
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
		DefaultCheckStatus: "OK",
		Host:               "",
		Methods:            []string{},
		Aggregator:         "",
		CheckName:          "",
		Description:        "",
		AggregatorKWargs:   JugglerAggregatorKWArgs{},
		Flap:               JugglerFlapConfig{},
		JPluginConfig:      configs.PluginConfig{},
		JHosts:             []string{},
		JFrontend:          []string{},
		Conditions:         Conditions{},
	}

}
