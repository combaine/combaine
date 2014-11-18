package agave

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"

	"github.com/mitchellh/mapstructure"
)

const urlTemplateString = "/api/update/{{.Group}}/{{.Graphname}}?values={{.Values}}&ts={{.Time}}&template={{.Template}}&title={{.Title}}&step={{.Step}}"

var URLTEMPLATE *template.Template = template.Must(template.New("URL").Parse(urlTemplateString))

const (
	CONNECTION_TIMEOUT = 2000 // ms
	RW_TIMEOUT         = 3000 // ms
)

var DEFAULT_HEADERS = http.Header{
	"User-Agent": {"Yandex/CombaineClient"},
	"Connection": {"TE"},
	"TE":         {"deflate", "gzip;q=0.3"},
}

type IAgaveSender interface {
	Send(tasks.DataType) error
}

type AgaveSender struct {
	AgaveConfig
}

type AgaveConfig struct {
	Id            string   `mapstructure:"Id"`
	Items         []string `mapstructure:"items"`
	Hosts         []string `mapstructure:"hosts"`
	GraphName     string   `mapstructure:"graph_name"`
	GraphTemplate string   `mapstructure:"graph_template"`
	Fields        []string `mapstructure:"Fields"`
	Step          int64    `mapstructure:"step"`
}

func (as *AgaveSender) Send(data tasks.DataType) (err error) {
	// Repack data by subgroups
	var repacked map[string][]string = make(map[string][]string)
	for _, aggname := range as.Items {
		for subgroup, value := range data[aggname] {
			rv := reflect.ValueOf(value)
			switch kind := rv.Kind(); kind {
			case reflect.Slice, reflect.Array:
				if len(as.Fields) == 0 || len(as.Fields) != rv.Len() {
					logger.Errf("%s Unable to send a slice. Fields len %d, len of value %d", as.Id, len(as.Fields), rv.Len())
					continue
				}
				forJoin := []string{}
				for i, field := range as.Fields {
					forJoin = append(forJoin, fmt.Sprintf("%s:%s", field, common.InterfaceToString(rv.Index(i).Interface())))
				}
				repacked[subgroup] = append(repacked[subgroup], strings.Join(forJoin, "+"))
			default:
				repacked[subgroup] = append(repacked[subgroup], fmt.Sprintf("%s:%s", aggname, common.InterfaceToString(value)))
			}
		}
	}

	//Send points
	for subgroup, value := range repacked {
		go as.handleOneItem(subgroup, strings.Join(value, "+"))
	}

	return
}

func (as *AgaveSender) handleOneItem(subgroup string, values string) {
	var url bytes.Buffer
	if err := URLTEMPLATE.Execute(&url, struct {
		Group     string
		Values    string
		Time      int64
		Template  string
		Title     string
		Graphname string
		Step      int64
	}{
		subgroup,
		values,
		time.Now().Unix(),
		as.GraphTemplate,
		as.GraphName,
		as.GraphName,
		as.Step,
	}); err != nil {
		logger.Errf("%s unable to generate template %s", as.Id, err)
		return
	}

	as.sendPoint(url.String())
}

func (as *AgaveSender) sendPoint(url string) {
	for _, host := range as.Hosts {
		client := httpclient.NewClientWithTimeout(
			time.Millisecond*CONNECTION_TIMEOUT,
			time.Millisecond*RW_TIMEOUT,
		)
		req, _ := http.NewRequest("GET",
			fmt.Sprintf("http://%s%s", host, url),
			nil)
		req.Header = DEFAULT_HEADERS

		logger.Debugf("%s %s", as.Id, req.URL)
		resp, err := client.Do(req)
		if err != nil {
			logger.Errf("%s Unable to do request %s", as.Id, err)
			continue
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Errf("%s %s %d %s", as.Id, req.URL, resp.StatusCode, err)
			continue
		}

		logger.Infof("%s %s %d %s", as.Id, req.URL, resp.StatusCode, body)
	}
}

func NewAgaveSender(config map[string]interface{}) (as IAgaveSender, err error) {
	var agave_config AgaveConfig
	if err = mapstructure.Decode(config, &agave_config); err != nil {
		return nil, err
	}

	as = &AgaveSender{
		AgaveConfig: agave_config,
	}
	return as, nil
}
