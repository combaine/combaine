package juggler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"unicode"

	"github.com/combaine/combaine/common/chttp"
	"github.com/combaine/combaine/common/logger"
	"github.com/pkg/errors"
	diff "github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
)

const (
	getChecksURL             = "http://%s/api/checks/checks?%s"
	updateCheckURL           = "http://%s/api/checks/add_or_update?do=1"
	sendEventURL             = "http://%s/juggler-fcgi.py?%s"
	notificationsDescription = "autocreated by combaine (github.com/combaine)"
)

var (
	defaultTags   = []string{"combaine"}
	golemTemplate = json.RawMessage(
		`{"template_name": "golem", "template_kwargs": {}, "description": "` + notificationsDescription + `"}`)
	smsTemplate = json.RawMessage(
		`{"template_name": "on_status_change", "template_kwargs": {
"golem_responsible": true, "method": ["sms"], "status": [
	{ "from": "OK", "to": "CRIT"}, { "from": "WARN", "to": "CRIT"},
	{ "from": "CRIT", "to": "WARN"}, { "from": "CRIT", "to": "OK"}
], "min_interval": 60 }, "description": "` + notificationsDescription + `"}`)
)

type jugglerResponse map[ /*hostname*/ string]map[ /*serviceName*/ string]jugglerCheck

type jugglerChildrenCheck struct {
	Instance string `json:"instance"`
	Host     string `json:"host"`
	Type     string `json:"type"`
	Service  string `json:"service"`
}

type jugglerFlapConfig struct {
	BoostTime    int64 `codec:"boost_time" json:"boost_time"`
	StableTime   int64 `codec:"stable_time" json:"stable_time"`
	CriticalTime int64 `codec:"critical_time" json:"critical_time"`
}

type jugglerCheck struct {
	update           bool
	Host             string                 `json:"host"`
	Service          string                 `json:"service"`
	Description      string                 `json:"description"`
	Aggregator       string                 `json:"aggregator"`
	AggregatorKWArgs json.RawMessage        `json:"aggregator_kwargs"`
	Notifications    []json.RawMessage      `json:"notifications"`
	TTL              int                    `json:"ttl"`
	Tags             []string               `json:"tags"`
	Children         []jugglerChildrenCheck `json:"children"`
	Flaps            *jugglerFlapConfig     `json:"flaps,omitempty"`
	Namespace        string                 `json:"namespace"`
}

func (c *jugglerCheck) needUpdated() {
	if !c.update {
		c.update = true
	}
}
func (c *jugglerCheck) modified() bool {
	return c.update
}

type jugglerEvent struct {
	taskTags    map[string]string
	Host        string   `json:"host"`
	Service     string   `json:"service"`
	Status      string   `json:"status"`
	Description string   `json:"description"`
	Instance    string   `json:"instance,omitempty"`
	Version     string   `json:"version,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

type jugglerBatchRequest struct {
	Events []jugglerEvent `json:"events"`
	Source string         `json:"source"`
}

type jugglerBatchResponse struct {
	Events []jugglerBatchEventReport `json:"events"`
	Error  *jugglerBatchError        `json:"error"`
}
type jugglerBatchEventReport struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}
type jugglerBatchError struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
	Label   string `json:"label"`
}

// getCheck query juggler api for check
// and Unmarshal json response in to jugglerResponse type
func (js *Sender) getCheck(ctx context.Context, events []jugglerEvent) (jugglerResponse, error) {
	if len(js.Tags) == 0 {
		js.Tags = defaultTags
		logger.Debugf("%s Set query tags to default %s", js.id, js.Tags)
	}
	//do=1&include_children=true&tag_name=combaine&host_name={js.Host}
	query := url.Values{
		"do":                    {"1"},
		"include_children":      {"true"},
		"include_notifications": {"true"},
		"tag_name":              defaultTags, // for query all known combainer checks
		"host_name":             {js.Host},
	}.Encode()

	checkFetcher := func() ([]byte, error) {
		var jerrors []error
		var cancel func()

		deadline, ok := ctx.Deadline()
		if ok {
			// copy context for background cache update
			ctx, cancel = context.WithDeadline(context.Background(), deadline)
			defer cancel()
		}
	GET_CHECK:
		for _, jhost := range js.JHosts {
			url := fmt.Sprintf(getChecksURL, jhost, query)
			logger.Infof("%s Query check %s", js.id, url)

			resp, err := chttp.Get(ctx, url)
			switch err {
			case nil:
				body, rerr := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if rerr != nil {
					jerrors = append(jerrors, errors.Errorf("%s: %s", jhost, rerr))
					continue
				}
				if resp.StatusCode != http.StatusOK {
					jerrors = append(jerrors, errors.Errorf("%s: %v %s", jhost, resp.StatusCode, body))
					continue
				}
				logger.Debugf("%s Juggler response: %s", js.id, body)
				return body, nil
			case context.Canceled, context.DeadlineExceeded:
				jerrors = append(jerrors, err)
				break GET_CHECK
			default:
				jerrors = append(jerrors, errors.Errorf("%s: %s", jhost, err))
				continue
			}
		}
		return nil, errors.Errorf("getCheck: Failed to get juggler check: %q", jerrors)
	}
	cJSON, err := GlobalCache.GetBytes(js.id, js.Host, checkFetcher)
	if err != nil {
		return nil, errors.Wrap(err, "getCheck")
	}
	var hostChecks jugglerResponse
	if err := json.Unmarshal(cJSON, &hostChecks); err != nil {
		logger.Errf("Failed to Unmarshal hostChecks: %s", err)
		hostChecks = jugglerResponse{js.Host: make(map[string]jugglerCheck)}
	}
	// XXX: do not remove this crutch, until you are 100% sure,
	// check json returned from api/checks/checks
	var flap map[string]map[string]*jugglerFlapConfig
	if err := json.Unmarshal(cJSON, &flap); err != nil {
		logger.Errf("Failed to Unmarshal flaps: %s", err)
		flap = map[string]map[string]*jugglerFlapConfig{
			js.Host: make(map[string]*jugglerFlapConfig),
		}
	}
	for c, v := range flap[js.Host] {
		if v.StableTime != 0 || v.CriticalTime != 0 || v.BoostTime != 0 {
			chk := hostChecks[js.Host][c]
			chk.Flaps = v
			hostChecks[js.Host][c] = chk
		}
	}
	return hostChecks, nil
}

// ensureCheck check that juggler check exists and it in sync with task data
// if need it call add_or_update check
func (js *Sender) ensureCheck(ctx context.Context, hostChecks jugglerResponse, triggers []jugglerEvent) error {
	services, ok := hostChecks[js.Host]
	if !ok {
		logger.Debugf("%s Create new checks for %s", js.id, js.Host)
		services = make(map[string]jugglerCheck)
		hostChecks[js.Host] = services
	}
	childSet := make(map[string]struct{}) // set
	for serviceName, v := range services {
		for _, c := range v.Children {
			childSet[c.Host+":"+serviceName] = struct{}{}
		}
	}
	updated := make(map[string]struct{})
	for _, t := range triggers {
		check, ok := services[t.Service]
		if !ok {
			logger.Infof("%s Add new check %s.%s", js.id, js.Host, t.Service)
			check = jugglerCheck{update: true}
		}

		if _, ok := updated[t.Service]; ok {
			continue // Ensure check only once.
		}
		updated[t.Service] = struct{}{}

		logger.Infof("%s Ensure check %s for %s", js.id, t.Service, js.Host)

		// ttl
		js.ensureTTL(&check)
		// aggregator
		err := js.ensureAggregator(&check)
		if err != nil {
			return errors.Wrap(err, "ensureCheck")
		}
		// methods
		err = js.ensureNotifications(&check)
		if err != nil {
			return errors.Wrap(err, "ensureCheck")
		}
		// flap
		js.ensureFlap(&check)
		// tags
		js.ensureTags(&check)
		// description
		js.ensureDescription(&check)
		// namespace
		js.ensureNamespace(&check)

		// add children
		if _, ok := childSet[t.Host+":"+t.Service]; !ok {
			check.needUpdated()
			child := jugglerChildrenCheck{
				Host:    t.Host,
				Type:    "HOST", // FIXME? hardcode, delete?
				Service: t.Service,
			}
			logger.Debugf("%s Add children %s", js.id, child)
			check.Children = append(check.Children, child)
		}

		if check.modified() {
			check.Host = js.Host
			check.Service = t.Service
			services[t.Service] = check
		}
	}
	cleanCache := false
	defer func() {
		// defer because cached item should be deleted after all check updated
		// or before return in case updateCheck ends with error
		if cleanCache {
			logger.Debugf("%s Clean cache for %s", js.id, js.Host)
			GlobalCache.Delete(js.Host)
		}
	}()
	for _, c := range services {
		if c.modified() {
			cleanCache = true
			if err := js.updateCheck(ctx, c); err != nil {
				return err
			}
		}
	}
	return nil
}

func (js *Sender) ensureTTL(c *jugglerCheck) {
	if js.TTL == 0 {
		js.TTL = 900 // default juggler ttl
	}
	if c.TTL != js.TTL {
		c.needUpdated()
		logger.Infof("%s Check outdated, ttl differ: %d != %d", js.id, c.TTL, js.TTL)
		c.TTL = js.TTL
	}
}

func (js *Sender) ensureDescription(c *jugglerCheck) {
	if js.Description == "" {
		js.Description = js.CheckName
	}
	if c.Description != js.Description && js.Description != "" {
		c.needUpdated()
		logger.Infof("%s Check outdated, description differ: '%s' != '%s'", js.id, c.Description, js.Description)
		c.Description = js.Description
	}
}

func (js *Sender) ensureAggregator(c *jugglerCheck) error {
	if c.Aggregator != js.Aggregator {
		c.needUpdated()
		logger.Infof("%s Check outdated, aggregator differ: %s != %s", js.id, c.Aggregator, js.Aggregator)
		c.Aggregator = js.Aggregator
	}
	configKWArgs, err := json.Marshal(js.AggregatorKWArgs)
	if err != nil {
		return errors.Wrap(err, "ensureAggregator: Marshal configKWArgs")
	}
	checkKWArgs := string(c.AggregatorKWArgs)
	if checkKWArgs == "" {
		checkKWArgs = "null"
	}
	// TODO(sakateka) add real tests, currently tested by reading logs
	d, err := diff.New().Compare(configKWArgs, []byte(checkKWArgs))
	if err != nil {
		return errors.Wrapf(err, "ensureAggregator: Failed to unmarshal configKWArgs: %s, checkKWArgs: %s", configKWArgs, checkKWArgs)
	}
	if d.Modified() {
		c.needUpdated()
		f := &formatter.DeltaFormatter{PrintIndent: false}
		diffString, err := f.Format(d)
		if err != nil {
			return errors.Wrap(err, "ensureAggregator: format diff")
		}
		diffString = diffString[:len(diffString)-1] // - \n
		// https://github.com/benjamine/jsondiffpatch/blob/master/docs/deltas.md
		logger.Infof("%s Check outdated, aggregator_kwargs differ: %s", js.id, diffString)
		c.AggregatorKWArgs = configKWArgs
	}
	return nil
}

func (js *Sender) ensureNotifications(c *jugglerCheck) error {
	notifications := make([]json.RawMessage, 0) // allocate to avoid comparison with {description: null}

	if js.Method != "" {
		if len(js.Notifications) != 0 {
			logger.Errf("%s 'Method' conflict with 'notifications', i'm ignore 'Method'", js.id)
		} else {
			logger.Infof("%s convert js.Method=%s to notifications", js.id, js.Method)
			for _, m := range strings.Split(js.Method, ",") {
				m = strings.TrimLeftFunc(m, unicode.IsSpace)
				if len(m) > 0 {
					m = strings.ToLower(m[:1])
					switch m {
					case "s":
						notifications = append(notifications, smsTemplate)
					case "g":
						notifications = append(notifications, golemTemplate)
					}
				}
			}
		}
	}

	for idx := range js.Notifications {
		n := js.Notifications[idx]
		if _, ok := n["description"]; !ok {
			n["description"] = notificationsDescription
		}
		notificationJSON, err := json.Marshal(n)
		if err != nil {
			return errors.Wrapf(err, "ensureNotifications: config.notifications[%d]=%#v", idx, n)
		}
		notifications = append(notifications, notificationJSON)
	}

	update := false

	// TODO(sakateka) add real tests, currently tested by reading logs
	if len(notifications) == len(c.Notifications) {
		nJSON, nErr := json.Marshal(map[string]interface{}{"notifications": notifications})
		if nErr != nil {
			return errors.Wrapf(nErr, "ensureNotifications: config.notifications=%#v", notifications)
		}

		if c.Notifications == nil {
			c.Notifications = make([]json.RawMessage, 0) // allocate to avoid comparison with {notifications: null}
		}
		cJSON, cErr := json.Marshal(map[string]interface{}{"notifications": c.Notifications})
		if cErr != nil {
			return errors.Wrapf(cErr, "ensureNotifications: check.notifications=%#v", c.Notifications)
		}

		d, err := diff.New().Compare(nJSON, cJSON)
		if err != nil {
			return errors.Wrapf(err, "ensureNotifications: unmarshal nJSON: %s, cJSON: %s", nJSON, cJSON)
		}
		if d.Modified() {
			// the server may sort the notifications, thereby moving the elements :-(
			// recheck for modifications
			// because the above is checked that the notification is arrays
			arrDiff := d.Deltas()[0].(*diff.Array)
		MODIFIED:
			for _, adelta := range arrDiff.Deltas {
				switch adelta.(type) {
				case *diff.Moved:
				default:
					update = true
					break MODIFIED
				}
			}
		}
		if update {
			f := &formatter.DeltaFormatter{PrintIndent: false}
			diffString, err := f.Format(d)
			if err != nil {
				return errors.Wrap(err, "ensureNotifications: format diff")
			}
			diffString = diffString[:len(diffString)-1] // - \n
			// https://github.com/benjamine/jsondiffpatch/blob/master/docs/deltas.md
			logger.Infof("%s Check outdated, notifications differ: %s", js.id, diffString)
		}
	} else {
		update = true
		logger.Infof("%s Check outdated, len(notifications) not match: config=%d, from juggler=%d",
			js.id, len(notifications), len(c.Notifications))
	}
	if update {
		c.needUpdated()
		c.Notifications = notifications
	}
	return nil
}

func (js *Sender) ensureFlap(c *jugglerCheck) {
	var inConfig *jugglerFlapConfig
	var msg string
	if f, ok := js.FlapsByChecks[c.Service]; ok {
		inConfig = &f
		msg = " (from FlapsByChecks)"
	} else {
		// if flap setting not set for check individually, try apply global settings
		inConfig = js.Flaps
	}
	if inConfig != nil && inConfig.StableTime == 0 && inConfig.CriticalTime == 0 && inConfig.BoostTime == 0 {
		logger.Infof("%s Check's flaps not configured, skip")
		inConfig = nil
	}

	var update bool
	if (c.Flaps == nil && inConfig != nil) ||
		(c.Flaps != nil && inConfig == nil) ||
		(c.Flaps != nil && inConfig != nil && *c.Flaps != *inConfig) {

		c.needUpdated()
		c.Flaps = inConfig
		update = true
	}

	if update {
		logger.Infof("%s Check outdated, Flaps%s differ: %s (juggler) != %s (inConfig)", js.id, msg, c.Flaps, inConfig)
	}
}

func (js *Sender) ensureTags(c *jugglerCheck) {
	if len(js.Tags) == 0 {
		js.Tags = defaultTags
	}
	// TODO: tags by servces in juggler config as for flaps?
	if c.Tags == nil || len(c.Tags) == 0 {
		c.needUpdated()
		logger.Infof("%s Check outdated, Tags differ: %s != %s", js.id, c.Tags, js.Tags)
		c.Tags = make([]string, len(js.Tags))
		copy(c.Tags, js.Tags)
	} else {
		tagsSet := make(map[string]struct{}, len(c.Tags))
		for _, tag := range c.Tags {
			tagsSet[tag] = struct{}{}
		}
		for _, tag := range js.Tags {
			if _, ok := tagsSet[tag]; !ok {
				c.needUpdated()
				logger.Infof("%s Add tag %s", js.id, tag)
				c.Tags = append(c.Tags, tag)
			}
		}
	}
}

func (js *Sender) ensureNamespace(c *jugglerCheck) {
	if js.Namespace == "" {
		js.Namespace = "combaine"
	}
	if js.Config.Token != "" && c.Namespace != js.Namespace {
		c.needUpdated()
		logger.Infof("%s Check outdated, namespace differ: '%s' != '%s'", js.id, c.Namespace, js.Namespace)
		c.Namespace = js.Namespace
	}
}

func (js *Sender) updateCheck(ctx context.Context, check jugglerCheck) error {
	logger.Infof("%s Update check %s for %s", js.id, check.Service, check.Host)

	cJSON, err := json.Marshal(check)
	if err != nil {
		return err
	}
	logger.Debugf("%s JSON payload for updating check: %s", js.id, cJSON)

	errs := make(map[string]string, 0)
	for _, host := range js.JHosts {
		url := fmt.Sprintf(updateCheckURL, host)
		req, err := http.NewRequest("POST", url, bytes.NewReader(cJSON))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Juggler sender (github.com/combaine)")
		if js.Config.Token != "" {
			req.Header.Set("Authorization", "OAuth "+js.Config.Token)
		}
		resp, err := chttp.Do(ctx, req.WithContext(ctx))
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
				logger.Warnf("%s Update check query %s: %d - %s", js.id, url, resp.StatusCode, body)
				errs[string(body)] = ""
				continue
			}
			logger.Infof("%s Sucessfully send update %s.%s %s: %s", js.id, check.Host, check.Service, url, body)
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
		return errors.Errorf("%s", errs)
	}
	logger.Errf("%s failed to sent update check for %v", js.id, check)
	return errors.Errorf("Upexpected error, can't update check for %s.%s", check.Host, check.Service)
}
