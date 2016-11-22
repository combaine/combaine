package juggler

import (
	"io/ioutil"
	"os"

	"github.com/combaine/combaine/common/tasks"

	"gopkg.in/yaml.v2"
)

const defaultConfigPath = "/etc/combaine/juggler.yaml"

type JugglerConfig struct {
	Type          string                 `codec:"type"`
	Host          string                 `codec:"Host"`
	Method        string                 `codec:"Method"`
	Aggregator    string                 `codec:"Aggregator"`
	CheckName     string                 `codec:"checkname"`
	Description   string                 `codec:"description"`
	JPluginConfig map[string]interface{} `codec:"config"`
	JHosts        []string               `yaml:"juggler_hosts"`
	JFrontend     []string               `yaml:"juggler_frontend"`
}

type jugglerServers struct {
	Hosts    []string `yaml:"juggler_hosts"`
	Frontend []string `yaml:"juggler_frontend"`
}

type JugglerSender struct {
}

// GetJugglerConfig read yaml file with two arrays of hosts
// if juggler_frontend not defined, use juggler_hosts as frontend
func GetJugglerConfig() (conf jugglerServers, err error) {
	var path string = os.Getenv("JUGGLER_CONFIG")
	if len(path) == 0 {
		path = defaultConfigPath
	}

	rawConfig, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}
	err = yaml.Unmarshal(rawConfig, &conf)
	if conf.Frontend == nil {
		conf.Frontend = conf.Hosts
	}
	return
}

func NewJugglerClient(conf *JugglerConfig, id string) (*JugglerSender, error) {
	return &JugglerSender{}, nil
}

func (js *JugglerSender) Send(data tasks.DataType) error {
	return nil
}
