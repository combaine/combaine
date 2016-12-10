package juggler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"

	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
)

const (
	GET_CHECK_URL = "http://%s/api/checks/checks?%s"
	AOU_CHECK_URL = "http://%s/api/checks/add_or_update?do=1"
	DEFAULT_TAG   = "combaine"
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
	Tags        map[string]string
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
	query := url.Values{
		"do":               {"1"},
		"include_children": {"true"},
		"tag_name":         js.JugglerConfig.Tags,
	}
	for _, jhost := range js.JHosts {
		//do=1&include_children=true&tag_name=combaine&host_name=
		query.Set("host_name", js.Host)
		url := fmt.Sprintf(GET_CHECK_URL, jhost, query.Encode())
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

// ensureCheck check that juggler check exists and it in sync with task data
// if need it call add_or_update check
func (js *jugglerSender) ensureCheck(ctx context.Context, hostChecks JugglerResponse, triggers []jugglerEvent) error {

	services, ok := hostChecks[js.Host]
	if !ok {
		logger.Debugf("%s Create new checks for host: %s", js.id, js.Host)
		services = make(map[string]JugglerCheck)
		hostChecks[js.Host] = services
	}
	childSet := make(map[string]struct{}) // set
	for serviceName, v := range services {
		for _, c := range v.Children {
			childSet[c.Host+":"+serviceName] = struct{}{}
		}
	}

	for _, t := range triggers {
		check, ok := services[t.Service]
		if !ok {
			check = JugglerCheck{Update: true}
		}
		if t.Tags["type"] == "metahost" {
			logger.Infof("%s ensure check %s for metahost %s", js.id, t.Service, t.Tags["metahost"])
			// aggregator
			if check.Aggregator != js.Aggregator ||
				!reflect.DeepEqual(check.AggregatorKWArgs, js.AggregatorKWargs) {
				check.Update = true
				check.Aggregator = js.Aggregator
				check.AggregatorKWArgs = js.AggregatorKWargs
			}
			// flap
			if err := js.ensureFlap(&check); err != nil {
				return err
			}

			// tags
			if js.JugglerConfig.Tags == nil || len(js.JugglerConfig.Tags) == 0 {
				js.JugglerConfig.Tags = []string{DEFAULT_TAG}
			}
			// TODO: tags by servces in juggler config as for flaps?
			if check.Tags == nil || len(check.Tags) == 0 {
				check.Update = true
				check.Tags = make([]string, len(js.JugglerConfig.Tags))
				copy(check.Tags, js.JugglerConfig.Tags)
			} else {
				tagsSet := make(map[string]struct{}, len(check.Tags))
				for _, tag := range check.Tags {
					tagsSet[tag] = struct{}{}
				}
				for _, tag := range js.JugglerConfig.Tags {
					if _, ok := tagsSet[tag]; !ok {
						check.Update = true
						logger.Warnf("%s Add tag %s for check %s", js.id, tag, t.Service)
						check.Tags = append(check.Tags, tag)
					}
				}
			}
		} else {
			name := t.Tags["metahost"]
			if t.Tags["type"] == "datacenter" {
				name = fmt.Sprintf("%s-%s-%s", name, js.Host, t.Tags["name"])
			} else { // type == host
				name = name + "-" + t.Tags["name"]
			}

			if _, ok := childSet[name+":"+t.Service]; !ok {
				check.Update = true
				child := JugglerChildrenCheck{
					Instance: "", // FIXME? hardcode, delete?
					Host:     name,
					Type:     "HOST", // FIXME? hardcode, delete?
					Service:  t.Service,
				}
				logger.Debugf("%s Add children %s", js.id, child)
				check.Children = append(check.Children, child)
			}
		}
		if check.Update {
			services[t.Service] = check
			check.Host = js.Host
			check.Service = t.Service
			if err := js.updateCheck(ctx, check); err != nil {
				return err
			}
		}
	}
	return nil
}

func (js *jugglerSender) ensureFlap(jcheck *JugglerCheck) error {
	if f, ok := js.JugglerConfig.ChecksOptions[jcheck.Service]; ok {
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

func (js *jugglerSender) updateCheck(ctx context.Context, check JugglerCheck) error {
	logger.Infof("%s Update check %s", js.id, check.Service)
	return nil
}

// sendEvent send juggler event borned by ensureCheck to juggler's
func (js *jugglerSender) sendEvent(event jugglerEvent) error {
	return nil
}
