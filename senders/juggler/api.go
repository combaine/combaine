package juggler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"

	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
)

const (
	getCheckUrl = "http://%s/api/checks/checks?do=1&include_children=true&host_name=%s"
	AoUCheckUrl = "http://%s/api/checks/add_or_update?do=1"
)

type JugglerResponse map[ /*hostname*/ string]map[ /*serviceName*/ string]JugglerCheck

type JugglerChildrenCheck struct {
	Instance string `json:"instance"`
	Host     string `json:"host"`
	Type     string `json:"type"`
	Service  string `json:"service"`
}

type JugglerFlapConfig struct {
	Enable       int64 `codec:"enable" json:"-"`
	BoostTime    int64 `codec:"boost_time" json:"boost_time"`
	StableTime   int64 `codec:"stable_time" json:"stable_time"`
	CriticalTime int64 `codec:"critical_time" json:"critical_time"`
}

type JugglerCheck struct {
	Update           bool                   `json:"-"`
	Host             string                 `json:"host"`
	Service          string                 `json:"service"`
	Description      string                 `json:"description"`
	Aggregator       string                 `json:"aggregator"`
	AggregatorKWArgs json.RawMessage        `json:"aggregator_kwargs"`
	Tags             []string               `json:"tags"`
	Methods          []string               `json:"methods"`
	Children         []JugglerChildrenCheck `json:"children"`
	Flap             *JugglerFlapConfig     `json:"flaps,omitempty"`

	//Active           string                  `json:"active"`
	//ActiveKWArgs     map[string]string       `json:"active_kwargs"`
	//AlertInterval    []int64                `json:"alert_interval"`
	//RefreshTime      int64                  `json:"refresh_time"`
	//Ttl              int64                  `json:"ttl"`
	//MaxStatus        string                  `json:"max_status"`
	//CreationTime     int64                   `json:"creation_time"`
	//ModificationTime int64                   `json:"modification_time"`
	//Notifications    []JugglerNotification   `json:"notifications"`
}

type jugglerEvent struct {
	Host        string
	Service     string
	Description string
	Level       int
}

/*
type JugglerNotification struct {
	TemplateName   string                 `json:"template_name"`
	TemplateKWArgs map[string]interface{} `json:"template_kwargs"`
	Description    string                 `json:"description"`
}
*/

// getCheck query juggler api for check and Unmarshal json response in to
// JugglerResponse type
func (js *jugglerSender) getCheck(ctx context.Context) (JugglerResponse, error) {
	var hostChecks JugglerResponse
	var flap map[string]map[string]*JugglerFlapConfig

	var jerrors []error
	for _, jhost := range js.JHosts {
		url := fmt.Sprintf(getCheckUrl, jhost, js.Host)
		logger.Infof("%s Query check %s", js.id, url)

		resp, err := httpclient.Get(ctx, url)
		switch err {
		case nil:
			body, rerr := ioutil.ReadAll(resp.Body)
			logger.Debugf("Juggler response %d: '%q'", resp.StatusCode, body)
			if rerr != nil {
				logger.Errf("%s %s", js.id, rerr)
				jerrors = append(jerrors, rerr)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				return nil, errors.New(string(body))
			}
			if err := json.Unmarshal(body, &hostChecks); err != nil {
				return nil, fmt.Errorf("Failed to Unmarshal hostChecks: %s", err)
			}
			if err := json.Unmarshal(body, &flap); err != nil {
				return nil, fmt.Errorf("Failed to Unmarshal flaps: %s", err)
			}
			for c, v := range flap[js.Host] {
				if v.StableTime != 0 || v.CriticalTime != 0 || v.BoostTime != 0 {
					chk := hostChecks[js.Host][c]
					chk.Flap = v
					hostChecks[js.Host][c] = chk
				}
			}
			return hostChecks, nil
		case context.Canceled, context.DeadlineExceeded:
			return nil, err
		default:
			logger.Errf("%s %s", js.id, err)
			jerrors = append(jerrors, err)
		}
	}
	return nil, fmt.Errorf("Failed to get juggler check: %q", jerrors)
}
func (js *jugglerSender) ensureFlap(jcheck *JugglerCheck) error {
	if f, ok := js.JugglerConfig.FlapByCheck[jcheck.Service]; ok {
		if f.Enable == 1 {
			if jcheck.Flap == nil {
				jcheck.Flap = &JugglerFlapConfig{Enable: 1}
				jcheck.Update = true
			}
			if jcheck.Flap != f {
				jcheck.Flap = f
				jcheck.Update = true
			}
		}
	} else {
		// if flap setting not set for check individually, try apply global settings
		if js.JugglerConfig.Flap != nil && js.JugglerConfig.Flap.Enable == 1 {
			if jcheck.Flap == nil {
				jcheck.Flap = &JugglerFlapConfig{Enable: 1}
				jcheck.Update = true
			}
			if jcheck.Flap != js.JugglerConfig.Flap {
				jcheck.Flap = js.JugglerConfig.Flap
				jcheck.Update = true
			}
		} else {
			jcheck.Flap = nil
		}
	}
	return nil
}

// ensureCheck check that juggler check exists and it in sync with task data
// if need it create or update check
func (js *jugglerSender) ensureCheck(ctx context.Context, hostChecks JugglerResponse, triggers []jugglerEvent) error {

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
