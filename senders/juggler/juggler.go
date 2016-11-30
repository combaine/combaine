package juggler

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/combaine/combaine/common/tasks"
	lua "github.com/yuin/gopher-lua"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultConfigPath = "/etc/combaine/juggler.yaml"
	PluginDir         = "/usr/lib/yandex/combaine/juggler"
)

type JugglerConfig struct {
	Host             string                  `codec:"Host"`
	Method           string                  `codec:"Method"`
	Aggregator       string                  `codec:"Aggregator"`
	CheckName        string                  `codec:"checkname"`
	Description      string                  `codec:"description"`
	AggregatorKWargs JugglerAggregatorKWArgs `codec:"aggregator_kwargs"`
	JPluginConfig    map[string]string       `codec:"config"`
	JHosts           []string                `codec:"juggler_hosts"`
	JFrontend        []string                `codec:"juggler_frontend"`
	Conditions       struct {
		OK   []string `codec:"OK",omit_empty`
		INFO []string `codec:"INFO",omit_empty`
		WARN []string `codec:"WARN",omit_empty`
		CRIT []string `codec:"CRIT",omit_empty`
	}
}

type jugglerServers struct {
	Hosts    []string `yaml:"juggler_hosts"`
	Frontend []string `yaml:"juggler_frontend"`
}

type jugglerSender struct {
	JugglerConfig
	id    string
	state *lua.LState
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

func NewJugglerClient(conf JugglerConfig, id string) (*jugglerSender, error) {
	return &jugglerSender{
		JugglerConfig: conf,
		id:            id,
		state:         lua.NewState(),
	}, nil
}

func (js *jugglerSender) preparePlugin(taskData tasks.DataType) error {
	// TODO: overwrite/cleanup globals in lua plugin?
	ltable, err := dataToLuaTable(js.state, taskData)
	if err != nil {
		return fmt.Errorf("%s Failed to convert taskData to lua table: %s", js.id, err)
	}

	js.state.SetGlobal("payload", ltable)

	levels := make(map[string][]string)
	if js.Conditions.OK != nil {
		levels["OK"] = js.Conditions.OK
	}
	if js.Conditions.INFO != nil {
		levels["INFO"] = js.Conditions.INFO
	}
	if js.Conditions.WARN != nil {
		levels["WARN"] = js.Conditions.WARN
	}
	if js.Conditions.CRIT != nil {
		levels["CRIT"] = js.Conditions.CRIT
	}

	lconditions := js.state.NewTable()
	idx := 0
	for name, cond := range levels {
		lcondTable := js.state.NewTable()
		lconditions.RawSetString(name, lua.LNumber(idx))
		lconditions.RawSetInt(idx, lcondTable)
		for _, v := range cond {
			lcondTable.Append(lua.LString(v))
		}
		idx++
	}
	js.state.SetGlobal("conditions", lconditions)
	return nil

	lconfig := js.state.NewTable()
	for k, v := range js.JPluginConfig {
		lconfig.RawSetString(k, lua.LString(v))
	}
	js.state.SetGlobal("config", lconfig)

	return nil
}

func (js *jugglerSender) runPlugin() error {
	// run lua plugin
	return nil
}

func (js *jugglerSender) Send(data tasks.DataType) error {
	if err := js.preparePlugin(data); err != nil {
		return err
	}

	return nil
}
