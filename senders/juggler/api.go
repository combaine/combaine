package juggler

import (
	"bytes"
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
	getChecksURL   = "http://%s/api/checks/checks?%s"
	updateCheckURL = "http://%s/api/checks/add_or_update?do=1"
	sendEventURL   = "http://%s/juggler-fcgi.py?%s"
	defaultTag     = "combaine"
)

type jugglerResponse map[ /*hostname*/ string]map[ /*serviceName*/ string]jugglerCheck

type jugglerChildrenCheck struct {
	Instance string `json:"instance"`
	Host     string `json:"host"`
	Type     string `json:"type"`
	Service  string `json:"service"`
}

type jugglerFlapConfig struct {
	Enable       int64 `codec:"enable" json:"-"`
	BoostTime    int64 `codec:"boost_time" json:"boost_time"`
	StableTime   int64 `codec:"stable_time" json:"stable_time"`
	CriticalTime int64 `codec:"critical_time" json:"critical_time"`
}

type jugglerCheck struct {
	Update           bool                   `json:"-"`
	Host             string                 `json:"host"`
	Service          string                 `json:"service"`
	Description      string                 `json:"description"`
	Aggregator       string                 `json:"aggregator"`
	AggregatorKWArgs interface{}            `json:"aggregator_kwargs"`
	Tags             []string               `json:"tags"`
	Methods          []string               `json:"methods"`
	Children         []jugglerChildrenCheck `json:"children"`
	Flap             *jugglerFlapConfig     `json:"flaps,omitempty"`
}

type jugglerEvent struct {
	Tags        map[string]string
	Service     string
	Description string
	Level       string
	Error       string
}

// getCheck query juggler api for check
// and Unmarshal json response in to jugglerResponse type
func (js *Sender) getCheck(ctx context.Context) (jugglerResponse, error) {
	var hostChecks jugglerResponse
	var flap map[string]map[string]*jugglerFlapConfig

	var jerrors []error
	query := url.Values{
		"do":               {"1"},
		"include_children": {"true"},
		"tag_name":         js.Config.Tags,
	}
	for _, jhost := range js.JHosts {
		//do=1&include_children=true&tag_name=combaine&host_name=
		query.Set("host_name", js.Host)
		url := fmt.Sprintf(getChecksURL, jhost, query.Encode())
		logger.Infof("%s Query check %s", js.id, url)

		resp, err := httpclient.Get(ctx, url)
		switch err {
		case nil:
			defer resp.Body.Close()
			body, rerr := ioutil.ReadAll(resp.Body)
			if rerr != nil {
				logger.Errf("%s %s", js.id, rerr)
				jerrors = append(jerrors, fmt.Errorf("failed to read respose from %s: %s", jhost, rerr))
				continue
			}
			logger.Debugf("%s Juggler response %d: '%q'", js.id, resp.StatusCode, body)

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
			logger.Errf("%s %s", js.id, err)
			return nil, err
		default:
			logger.Errf("%s %s", js.id, err)
			jerrors = append(jerrors, fmt.Errorf("host %s failed with %s", jhost, err))
			continue
		}
	}
	return nil, fmt.Errorf("Failed to get juggler check: %q", jerrors)
}

// ensureCheck check that juggler check exists and it in sync with task data
// if need it call add_or_update check
func (js *Sender) ensureCheck(ctx context.Context, hostChecks jugglerResponse, triggers []jugglerEvent) error {

	services, ok := hostChecks[js.Host]
	if !ok {
		logger.Debugf("%s Create new checks for host: %s", js.id, js.Host)
		services = make(map[string]jugglerCheck)
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
			check = jugglerCheck{Update: true}
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
			js.ensureFlap(&check)
			// tags
			if len(js.Config.Tags) == 0 {
				js.Config.Tags = []string{defaultTag}
			}
			// TODO: tags by servces in juggler config as for flaps?
			if check.Tags == nil || len(check.Tags) == 0 {
				check.Update = true
				check.Tags = make([]string, len(js.Config.Tags))
				copy(check.Tags, js.Config.Tags)
			} else {
				tagsSet := make(map[string]struct{}, len(check.Tags))
				for _, tag := range check.Tags {
					tagsSet[tag] = struct{}{}
				}
				for _, tag := range js.Config.Tags {
					if _, ok := tagsSet[tag]; !ok {
						check.Update = true
						logger.Warnf("%s Add tag %s for check %s", js.id, tag, t.Service)
						check.Tags = append(check.Tags, tag)
					}
				}
			}
		} else {
			name := t.Tags["metahost"] + "-" + t.Tags["name"]
			if t.Tags["type"] == "datacenter" {
				name = fmt.Sprintf("%s-%s-%s", t.Tags["metahost"], js.Host, t.Tags["name"])
			}
			t.Tags["name"] = name

			if _, ok := childSet[name+":"+t.Service]; !ok {
				check.Update = true
				child := jugglerChildrenCheck{
					Host:    name,
					Type:    "HOST", // FIXME? hardcode, delete?
					Service: t.Service,
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

func (js *Sender) ensureFlap(jcheck *jugglerCheck) {
	if f, ok := js.Config.ChecksOptions[jcheck.Service]; ok {
		if f.Enable == 1 {
			if jcheck.Flap == nil {
				jcheck.Flap = &jugglerFlapConfig{Enable: 1}
				jcheck.Update = true
			}
			if *jcheck.Flap != f {
				jcheck.Flap = &f
				jcheck.Update = true
			}
		}
	} else {
		// if flap setting not set for check individually, try apply global settings
		if js.Config.Flap != nil && js.Config.Flap.Enable == 1 {
			if jcheck.Flap == nil {
				jcheck.Flap = &jugglerFlapConfig{Enable: 1}
				jcheck.Update = true
			}
			if jcheck.Flap != js.Config.Flap {
				jcheck.Flap = js.Config.Flap
				jcheck.Update = true
			}
		} else {
			jcheck.Flap = nil
		}
	}
}

func (js *Sender) updateCheck(ctx context.Context, check jugglerCheck) error {
	logger.Infof("%s Update check %s", js.id, check.Service)

	cJSON, err := json.Marshal(check)
	if err != nil {
		return err
	}
	logger.Infof("%s Update check json payload: %s", js.id, cJSON)

	errs := make(map[string]string, 0)
	for _, host := range js.JHosts {
		url := fmt.Sprintf(updateCheckURL, host)
		resp, err := httpclient.Post(ctx, url, "application/json", bytes.NewReader(cJSON))
		switch err {
		case nil:
			body, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				logger.Errf("%s %s", js.id, err)
				errs[err.Error()] = ""
				continue
			}

			if resp.StatusCode != http.StatusOK {
				logger.Warnf("%s Update check query %s: %d - '%s'", js.id, url, resp.StatusCode, body)
				errs[string(body)] = ""
				continue
			}
			logger.Infof("%s Sucessfully send update `%s.%s` %s: %s", js.id, check.Host, check.Service, url, body)
			return nil
		case context.Canceled, context.DeadlineExceeded:
			logger.Errf("%s %s", js.id, err)
			return err
		default:
			logger.Errf("%s %s", js.id, err)
			errs[err.Error()] = ""
			continue
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", errs)
	}
	logger.Errf("%s failed to sent update check for %v", js.id, check)
	return fmt.Errorf("Upexpected error, can't update check for `%s.%s`", check.Host, check.Service)
}

// sendEvent send juggler event borned by ensureCheck to juggler's
func (js *Sender) sendEvent(ctx context.Context, front string, event jugglerEvent) error {
	logger.Infof("%s Send juggler event %s", js.id, event)
	query := url.Values{
		"status":      {event.Level},
		"description": {event.Description},
		"service":     {event.Service},
		"host":        {event.Tags["name"]},
		"instance":    {""},
	}

	url := fmt.Sprintf(sendEventURL, front, query.Encode())
	logger.Debugf("%s Try send event %s", js.id, url)
	resp, err := httpclient.Get(ctx, url)
	if err != nil {
		logger.Errf("%s %s", js.id, err)
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	logger.Debugf("%s Juggler response %d: '%q'", js.id, resp.StatusCode, body)
	if err != nil {
		logger.Errf("%s %s", js.id, err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}
	return nil
}
