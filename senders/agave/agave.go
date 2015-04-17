package agave

// IT"S ABSOLUTE SHIT! I MUST REWRITE IT!!! I WROTE IT WHEN I DID NOT KNOW GO! PLEASE, DO NOT PUSH ME!!!

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
)

const (
	CONNECTION_TIMEOUT = 2000 // ms
	RW_TIMEOUT         = 3000 // ms

	urlTemplateString = "/api/update/{{.Group}}/{{.Graphname}}?values={{.Values}}&ts={{.Time}}&template={{.Template}}&title={{.Title}}&step={{.Step}}"
)

var (
	AgaveHttpClient = httpclient.NewClientWithTimeout(
		time.Millisecond*CONNECTION_TIMEOUT,
		time.Millisecond*RW_TIMEOUT)

	DEFAULT_HEADERS = http.Header{
		"User-Agent": {"Yandex/CombaineClient"},
		"Connection": {"TE"},
		"TE":         {"deflate", "gzip;q=0.3"},
	}

	URLTEMPLATE *template.Template = template.Must(template.New("URL").Parse(urlTemplateString))
)

type IAgaveSender interface {
	Send(tasks.DataType) error
}

type AgaveSender struct {
	AgaveConfig
}

type AgaveConfig struct {
	Id            string   `codec:"Id"`
	Items         []string `codec:"items"`
	Hosts         []string `codec:"hosts"`
	GraphName     string   `codec:"graph_name"`
	GraphTemplate string   `codec:"graph_template"`
	Fields        []string `codec:"Fields"`
	Step          int64    `codec:"step"`
}

func (as *AgaveSender) Send(data tasks.DataType) error {

	repacked, err := as.send(data)
	if err != nil {
		return err
	}

	//Send points
	for subgroup, value := range repacked {
		go as.handleOneItem(subgroup, strings.Join(value, "+"))
	}

	return nil
}

func (as *AgaveSender) send(data tasks.DataType) (map[string][]string, error) {
	// Repack data by subgroups
	logger.Debugf("%s Data to send: %v", as.Id, data)
	var repacked map[string][]string = make(map[string][]string)
	for _, aggname := range as.Items {
		for subgroup, value := range data[aggname] {
			rv := reflect.ValueOf(value)
			switch rv.Kind() {
			case reflect.Slice, reflect.Array:
				if len(as.Fields) == 0 || len(as.Fields) != rv.Len() {
					logger.Errf("%s Unable to send a slice. Fields len %d, len of value %d", as.Id, len(as.Fields), rv.Len())
					continue
				}

				forJoin := make([]string, 0, len(as.Fields))
				for i, field := range as.Fields {
					forJoin = append(forJoin, fmt.Sprintf("%s:%s", field, common.InterfaceToString(rv.Index(i).Interface())))
				}

				repacked[subgroup] = append(repacked[subgroup], strings.Join(forJoin, "+"))
			case reflect.Map:
				keys := rv.MapKeys()
				for _, key := range keys {
					value := reflect.ValueOf(rv.MapIndex(key).Interface())

					switch value.Kind() {
					case reflect.Slice, reflect.Array:
						if len(as.Fields) == 0 || len(as.Fields) != value.Len() {
							logger.Errf("%s Unable to send a slice. Fields len %d, len of value %d", as.Id, len(as.Fields), rv.Len())
							continue
						}
						forJoin := make([]string, 0, len(as.Fields))
						for i, field := range as.Fields {
							forJoin = append(forJoin, fmt.Sprintf("%s_%s:%s", key, field, common.InterfaceToString(value.Index(i).Interface())))
						}
						repacked[subgroup] = append(repacked[subgroup], strings.Join(forJoin, "+"))
					default:
						repacked[subgroup] = append(repacked[subgroup], fmt.Sprintf("%s_%s:%s", aggname, key, common.InterfaceToString(value.Interface())))
					}

				}
			default:
				repacked[subgroup] = append(repacked[subgroup], fmt.Sprintf("%s:%s", aggname, common.InterfaceToString(value)))
			}
		}
	}

	return repacked, nil
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
		req, _ := http.NewRequest("GET",
			fmt.Sprintf("http://%s%s", host, url),
			nil)
		req.Header = DEFAULT_HEADERS

		logger.Debugf("%s %s", as.Id, req.URL)
		resp, err := AgaveHttpClient.Do(req)
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

func NewAgaveSender(config AgaveConfig) (as IAgaveSender, err error) {
	logger.Debugf("%s AgaveConfig: %s", config.Id, config)
	as = &AgaveSender{
		AgaveConfig: config,
	}
	return as, nil
}
