package agave

// IT"S ABSOLUTE SHIT! I MUST REWRITE IT!!! I WROTE IT WHEN I DID NOT KNOW GO! PLEASE, DO NOT PUSH ME!!!

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
)

const (
	connectionTimeout = 2000 // ms
	rwTimeout         = 3000 // ms

	urlTemplateString = "/api/update/{{.Group}}/{{.Graphname}}?values={{.Values}}&ts={{.Time}}&template={{.Template}}&title={{.Title}}&step={{.Step}}"
)

var (
	agaveHTTPClient = httpclient.NewClientWithTimeout(
		time.Millisecond*connectionTimeout,
		time.Millisecond*rwTimeout)

	defaultHeaders = http.Header{
		"User-Agent": {"Yandex/CombaineClient"},
		"Connection": {"TE"},
		"TE":         {"deflate", "gzip;q=0.3"},
	}

	urlTemplate = template.Must(template.New("URL").Parse(urlTemplateString))
)

// Sender is agave sender, embed agave config and provide method Send
type Sender struct {
	Config
}

// Config contains main configuration for agave sender
type Config struct {
	ID            string   `codec:"Id"`
	Items         []string `codec:"items"`
	Hosts         []string `codec:"hosts"`
	GraphName     string   `codec:"graph_name"`
	GraphTemplate string   `codec:"graph_template"`
	Fields        []string `codec:"Fields"`
	Step          int64    `codec:"step"`
}

// Send get task data and send all metrics to agave hosts, specified via config
func (as *Sender) Send(data []tasks.AggregationResult) error {

	repacked, err := as.send(data)
	if err != nil {
		return err
	}

	//Send points
	e := make(chan error, 1)
	errs := make(map[string]struct{}, 0)

	var wg sync.WaitGroup
	for subgroup, value := range repacked {
		wg.Add(1)
		go as.handleOneItem(subgroup, strings.Join(value, "+"), &wg, e)
	}

	go func() {
		wg.Wait()
		close(e)
	}()

	for err := range e {
		errs[fmt.Sprintf("%s", err)] = struct{}{}
	}
	if len(errs) > 0 {
		checkByHosts := len(repacked) * len(as.Hosts)
		if len(errs) == checkByHosts {
			return fmt.Errorf("%s", errs)
		}
		logger.Warnf("Failed to send %d/%d checks", len(errs), checkByHosts)
	}
	return nil
}

func (as *Sender) getSubgroupName(task tasks.AggregationResult) (string, error) {
	var subgroup string
	var ok bool

	if subgroup, ok = task.Tags["name"]; !ok {
		return "", fmt.Errorf("Failed to get data tag 'name', skip task: %v", task)
	}
	if t, ok := task.Tags["type"]; ok {
		if t == "datacenter" {
			if meta, ok := task.Tags["metahost"]; ok {
				subgroup = fmt.Sprintf("%s-%s", meta, subgroup) // meta.host.name + DC1
			} else {
				return "", fmt.Errorf("Failed to get data tag 'metahost', skip task: %v", task)
			}
		}
	} else {
		return "", fmt.Errorf("Failed to get data tag 'type', skip task: %v", task)
	}
	return subgroup, nil
}

func (as *Sender) send(data []tasks.AggregationResult) (map[string][]string, error) {
	// Repack data by subgroups
	logger.Debugf("%s Data to send: %v", as.ID, data)
	var repacked = make(map[string][]string)
	var queryItems = make(map[string][]string)
	for _, aggname := range as.Items {
		items := strings.SplitN(aggname, ".", 2)
		if len(items) > 1 {
			queryItems[items[0]] = append(queryItems[items[0]], items[1])
		} else {
			if _, ok := queryItems[items[0]]; !ok {
				queryItems[items[0]] = []string{}
			}
		}
	}
	for _, item := range data {
		var root string
		var metricname []string
		var ok bool

		if root, ok = item.Tags["aggregate"]; !ok {
			logger.Errf("%s Failed to get data tag 'aggregate', skip task: %v", as.ID, item)
			continue
		}
		if metricname, ok = queryItems[root]; !ok {
			logger.Debugf("%s %s not in Items, skip task: %v", as.ID, root, item)
			continue
		}
		subgroup, err := as.getSubgroupName(item)
		if err != nil {
			logger.Errf("%s %s", as.ID, err)
			continue
		}

		rv := reflect.ValueOf(item.Result)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			if len(metricname) != 0 {
				// we expect neted map here
				continue
			}
			if len(as.Fields) == 0 || len(as.Fields) != rv.Len() {
				logger.Errf("%s Unable to send a slice. Fields len %d, len of value %d", as.ID, len(as.Fields), rv.Len())
				continue
			}

			forJoin := make([]string, 0, len(as.Fields))
			for i, field := range as.Fields {
				forJoin = append(forJoin, fmt.Sprintf("%s:%s", field, common.InterfaceToString(rv.Index(i).Interface())))
			}

			repacked[subgroup] = append(repacked[subgroup], strings.Join(forJoin, "+"))
		case reflect.Map:
			if len(metricname) == 0 {
				continue
			}

			for _, mname := range metricname {

				key := reflect.ValueOf(mname)
				mapVal := rv.MapIndex(key)
				if !mapVal.IsValid() {
					continue
				}

				value := reflect.ValueOf(mapVal.Interface())

				switch value.Kind() {
				case reflect.Slice, reflect.Array:
					if len(as.Fields) == 0 || len(as.Fields) != value.Len() {
						logger.Errf("%s Unable to send a slice. Fields len %d, len of value %d",
							as.ID, len(as.Fields), rv.Len())
						continue
					}
					forJoin := make([]string, 0, len(as.Fields))
					for i, field := range as.Fields {
						forJoin = append(forJoin, fmt.Sprintf("%s:%s",
							field, common.InterfaceToString(value.Index(i).Interface())))
					}
					repacked[subgroup] = append(repacked[subgroup], strings.Join(forJoin, "+"))
				case reflect.Map:
					//unsupported
				default:
					repacked[subgroup] = append(repacked[subgroup], fmt.Sprintf("%s:%s",
						mname, common.InterfaceToString(value.Interface())))
				}
			}
		default:
			repacked[subgroup] = append(repacked[subgroup], fmt.Sprintf("%s:%s",
				root, common.InterfaceToString(item.Result)))
		}
	}

	return repacked, nil
}

func (as *Sender) handleOneItem(subgroup string, values string, g *sync.WaitGroup, e chan<- error) {
	var url bytes.Buffer
	defer g.Done()

	err := urlTemplate.Execute(&url, struct {
		Group     string
		Values    string
		Time      int64
		Template  string
		Title     string
		Graphname string
		Step      int64
	}{subgroup, values, time.Now().Unix(), as.GraphTemplate, as.GraphName, as.GraphName, as.Step})

	if err != nil {
		logger.Errf("%s unable to generate template %s", as.ID, err)
		e <- err
		return
	}

	g.Add(1)
	as.sendPoint(url.String(), g, e)
}

func (as *Sender) sendPoint(url string, g *sync.WaitGroup, e chan<- error) {
	defer g.Done()
	for _, host := range as.Hosts {
		req, _ := http.NewRequest("GET",
			fmt.Sprintf("http://%s%s", host, url),
			nil)
		req.Header = defaultHeaders

		logger.Debugf("%s %s", as.ID, req.URL)
		resp, err := agaveHTTPClient.Do(req)
		if err != nil {
			logger.Errf("%s Unable to do request %s", as.ID, err)
			e <- err
			continue
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Errf("%s %s %d %s", as.ID, req.URL, resp.StatusCode, err)
			e <- err
			continue
		}

		logger.Infof("%s %s %d %s", as.ID, req.URL, resp.StatusCode, body)
	}
}

// NewSender return agave sender interface
func NewSender(config Config) (as *Sender, err error) {
	logger.Debugf("%s Config: %s", config.ID, config)
	as = &Sender{
		Config: config,
	}
	return as, nil
}
