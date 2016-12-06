package juggler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
	lua "github.com/yuin/gopher-lua"
	yaml "gopkg.in/yaml.v2"
)

const (
	DEFAULT_CONFIG_PATH = "/etc/combaine/juggler.yaml"
	PLUGIN_DIR          = "/usr/lib/yandex/combaine/juggler"
)

type Conditions struct {
	OK   []string `codec:"OK"`
	INFO []string `codec:"INFO"`
	WARN []string `codec:"WARN"`
	CRIT []string `codec:"CRIT"`
}

type JugglerConfig struct {
	Host             string                  `codec:"Host"`
	Methods          []string                `codec:"Methods"`
	Aggregator       string                  `codec:"Aggregator"`
	CheckName        string                  `codec:"checkname"`
	Description      string                  `codec:"description"`
	AggregatorKWargs JugglerAggregatorKWArgs `codec:"aggregator_kwargs"`
	Flap             JugglerFlapConfig       `codec:"flap"`
	JPluginConfig    configs.PluginConfig    `codec:"config"`
	JHosts           []string                `codec:"juggler_hosts"`
	JFrontend        []string                `codec:"juggler_frontend"`
	Conditions
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
	return
}

func NewJugglerSender(conf JugglerConfig, id string) (*jugglerSender, error) {
	return &jugglerSender{
		JugglerConfig: conf,
		id:            id,
		state:         lua.NewState(),
	}, nil
}

// loadPlugin cleanup lua state global/local environment
// and load lua plugin by name from juggler config section
func (js *jugglerSender) loadPlugin() error {
	// TODO: overwrite/cleanup globals in lua plugin?
	// prelad all plugins and cache lua state
	return nil
}

// preparePluginEnv add data from aggregate task as global variable in lua
// plugin. Also inject juggler conditions from juggler configs and plugin config
func (js *jugglerSender) preparePluginEnv(taskData tasks.DataType) error {
	ltable, err := dataToLuaTable(js.state, taskData)
	if err != nil {
		return fmt.Errorf("Failed to convert taskData to lua table: %s", err)
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

	if lconfig, err := jPluginConfigToLuaTable(js.state, js.JPluginConfig); err != nil {
		return err
	} else {
		js.state.SetGlobal("config", lconfig)
	}
	return nil
}

// runPlugin run lua plugin with prepared environment
// collect, convert and return plugin result
func (js *jugglerSender) runPlugin() ([]jugglerEvent, error) {
	js.state.Push(js.state.GetGlobal("run"))
	if err := js.state.PCall(0, 1, nil); err != nil {
		return nil, err
	}
	return nil, nil
}

// getCheck query juggler api for check and Unmarshal json response in to
// JugglerResponse type
func (js *jugglerSender) getCheck(ctx context.Context) (JugglerResponse, error) {
	var hostChecks JugglerResponse
	var flap map[string]map[string]JugglerFlapConfig

	for jhost := range js.JHosts {
		url := fmt.Sprintf(getCheckUrl, jhost, js.Host)
		logger.Infof("%s Query check %s", js.id, url)

		resp, err := httpclient.Get(ctx, url)
		switch err {
		case nil:
			body, rerr := ioutil.ReadAll(resp.Body)
			if rerr != nil {
				logger.Errf("%s %s", js.id, rerr)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				return nil, errors.New(string(body))
			}
			if err := json.Unmarshal(body, &hostChecks); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(body, &flap); err != nil {
				return nil, err
			}
			for c, v := range flap[js.Host] {
				chk := hostChecks[js.Host][c]
				chk.Flap = v
				hostChecks[js.Host][c] = chk
			}
			return hostChecks, nil
		case context.Canceled, context.DeadlineExceeded:
			return nil, err
		default:
			logger.Errf("%s %s", js.id, err)
		}
	}

	return nil, errors.New("Failed to get juggler check")

}
func (js *jugglerSender) ensureFlap(jcheck *JugglerCheck) error {
	if js.JugglerConfig.Flap.Enable == 1 {
		jcheck.Flap.Enable = 1
		if jcheck.Flap != js.JugglerConfig.Flap {
			jcheck.Flap = js.JugglerConfig.Flap
			jcheck.Update = true
		}
	} else {
		jcheck.Flap = JugglerFlapConfig{}
	}
	return nil
}

// ensureCheck check that juggler check exists and it in sync with task data
// if need it create or update check
func (js *jugglerSender) ensureCheck(ctx context.Context, triggers []jugglerEvent) error {
	hostChecks, err := js.getCheck(ctx)
	if err != nil {
		return err
	}

	services, ok := hostChecks[js.Host]
	if !ok {
		services = make(map[string]JugglerCheck)
		hostChecks[js.Host] = services
	}
	childSet := make(map[string]struct{}) // set
	for n, v := range services {
		for _, c := range v.Children {
			childSet[c.Host+":"+n] = struct{}{}
		}
	}

	for _, t := range triggers {
		check, ok := services[t.Service]
		if !ok {
			check = JugglerCheck{Update: true}
		}
		if t.Host == js.Host { // for metahost
			if err := js.ensureFlap(&check); err != nil {
				return err
			}
			if check.Aggregator != js.Aggregator ||
				!reflect.DeepEqual(check.AggregatorKWArgs, js.AggregatorKWargs) {

				check.Update = true
				check.Aggregator = js.Aggregator
				check.AggregatorKWArgs = js.AggregatorKWargs
			}

		} else {
			if _, ok := childSet[t.Host+":"+t.Service]; !ok {
				check.Update = true
				check.Children = append(check.Children, JugglerChildrenCheck{
					Host:    t.Host,
					Service: t.Service,
				})
			}
		}
		if check.Update {
			if err := js.updateCheck(ctx, check); err != nil {
				return err
			}
		}
	}

	return nil
}

func (js *jugglerSender) updateCheck(ctx context.Context, check JugglerCheck) error {
	return nil
}

// sendEvent send juggler event borned by ensureCheck to juggler's
func (js *jugglerSender) sendEvent(event jugglerEvent) error {
	return nil
}

// Send make all things abount juggler sender tasks
func (js *jugglerSender) Send(data tasks.DataType) error {
	if err := js.loadPlugin(); err != nil {
		return err
	}
	if err := js.preparePluginEnv(data); err != nil {
		return err
	}

	jEvents, err := js.runPlugin()
	if err != nil {
		return err
	}
	if err := js.ensureCheck(context.TODO(), jEvents); err != nil {
		return err
	}

	var jWg sync.WaitGroup
	var sendEeventsFailed int32
	// TODO send evnets to all juggler fronts
	for _, e := range jEvents {
		jWg.Add(1)
		go func(jEv jugglerEvent, wg *sync.WaitGroup) {
			defer wg.Done()
			if err := js.sendEvent(jEv); err != nil {
				atomic.AddInt32(&sendEeventsFailed, 1)
				logger.Errf("%s failed to send juggler Event: %s", js.id, err)
			}
		}(e, &jWg)
	}
	jWg.Wait()

	if sendEeventsFailed > 0 {
		msg := fmt.Errorf("failed to send %d/%d events", sendEeventsFailed, len(jEvents))
		logger.Errf("%s %s", js.id, msg.Error())
		return msg
	}
	logger.Infof("%s successfully send %d events", js.id, len(jEvents))

	return nil
}
