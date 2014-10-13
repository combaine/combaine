package agave

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/httpclient"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

var (
	log      *cocaine.Logger
	logMutex sync.Mutex
)

func lazyLoggerInitialization() (*cocaine.Logger, error) {
	var err error
	if log != nil {
		return log, nil
	} else {
		logMutex.Lock()
		defer logMutex.Unlock()
		if log != nil {
			return log, nil
		}
		log, err = cocaine.NewLogger()
		return log, err
	}
}

const urlTemplateString = "/api/update/{{.Group}}/{{.Graphname}}?values={{.Values}}&ts={{.Time}}&template={{.Template}}&title={{.Title}}&step={{.Step}}"

var URLTEMPLATE *template.Template = template.Must(template.New("URL").Parse(urlTemplateString))

const CONNECTION_TIMEOUT = 2000
const RW_TIMEOUT = 3000

var DEFAULT_HEADERS = http.Header{
	"User-Agent": {"Yandex/CombaineClient"},
	"Connection": {"TE"},
	"TE":         {"deflate", "gzip;q=0.3"},
}

func missingCfgParametrError(param string) error {
	return fmt.Errorf("Missing configuration parametr: %s", param)
}

func wrongCfgParametrError(param string) error {
	return fmt.Errorf("Wrong type of parametr: %s", param)
}

// type DataItem map[string]interface{}
// type DataType map[string]DataItem

type AgaveSender struct {
	// Handled items in data. Only this will be handled.
	items         []string
	graphName     string
	graphTemplate string
	hosts         []string
	logger        *cocaine.Logger
	fields        []string
	step          int64
}

func (as *AgaveSender) Send(data common.DataType) (err error) {
	// Repack data by subgroups
	var repacked map[string][]string = make(map[string][]string)
	for _, aggname := range as.items {
		for subgroup, value := range data[aggname] {
			rv := reflect.ValueOf(value)
			switch kind := rv.Kind(); kind {
			case reflect.Slice, reflect.Array:
				if len(as.fields) == 0 || len(as.fields) != rv.Len() {
					as.logger.Errf("Unable to send a slice. Fields len %d, len of value %d", len(as.fields), rv.Len())
					continue
				}
				forJoin := []string{}
				for i, field := range as.fields {
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
	err := URLTEMPLATE.Execute(&url, struct {
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
		as.graphTemplate,
		as.graphName,
		as.graphName,
		as.step,
	})
	if err != nil {
		as.logger.Errf("%s", err)
	} else {
		as.sendPoint(url.String())
	}
}

func (as *AgaveSender) sendPoint(url string) {
	for _, host := range as.hosts {
		client := httpclient.NewClientWithTimeout(
			time.Millisecond*CONNECTION_TIMEOUT,
			time.Millisecond*RW_TIMEOUT)
		URL := fmt.Sprintf("http://%s%s", host, url)
		req, _ := http.NewRequest("GET", URL, nil)
		req.Header = DEFAULT_HEADERS
		as.logger.Debugf("%s", req.URL)
		_ = client
		if resp, err := client.Do(req); err != nil {
			as.logger.Errf("Unable to create request %s", err)
		} else {
			defer resp.Body.Close()
			if body, err := ioutil.ReadAll(resp.Body); err != nil {
				as.logger.Errf("%s %d %s", URL, resp.StatusCode, err)
			} else {
				as.logger.Infof("%s %d %s", URL, resp.StatusCode, body)
			}
		}
	}
}

func (as *AgaveSender) Close() (err error) {
	as.logger.Close()
	return
}

type IAgaveSender interface {
	Send(common.DataType) error
	Close() error
}

func NewAgaveSender(config map[string]interface{}) (as IAgaveSender, err error) {
	//items
	var items []string
	if cfgItems, ok := config["items"]; !ok {
		return nil, missingCfgParametrError("items")
	} else {
		if items, ok = cfgItems.([]string); !ok {
			return nil, wrongCfgParametrError("items")
		}
	}
	//
	var hosts []string
	if cfgHosts, ok := config["hosts"]; !ok {
		return nil, missingCfgParametrError("hosts")
	} else {
		if hosts, ok = cfgHosts.([]string); !ok {
			return nil, wrongCfgParametrError("hosts")
		}
	}
	//graphName
	var graphname string
	if cfgGraphName, ok := config["graph_name"]; !ok {
		return nil, missingCfgParametrError("graph_name")
	} else {
		if graphname, ok = cfgGraphName.(string); !ok {
			return nil, wrongCfgParametrError("graph_name")
		}
	}
	//graphTemplate
	var graphtemplate string
	if cfgGraphTeml, ok := config["graph_template"]; !ok {
		return nil, missingCfgParametrError("graph_template")
	} else {
		if graphtemplate, ok = cfgGraphTeml.(string); !ok {
			return nil, wrongCfgParametrError("graph_template")
		}
	}

	fields := []string{}
	if cfgFields, ok := config["Fields"]; ok {
		if fields, ok = cfgFields.([]string); !ok {
			return nil, wrongCfgParametrError("Fields")
		}
	}

	var step int64
	if cfgStep, ok := config["step"]; !ok {
		return nil, missingCfgParametrError("step")
	} else {
		if step, ok = cfgStep.(int64); !ok {
			return nil, wrongCfgParametrError("step")
		}
	}

	logger, err := lazyLoggerInitialization()
	if err != nil {
		return nil, err
	}
	logger.Debugf("Goroutine num %d", runtime.NumGoroutine())
	logger.Debugf("Step %v %v", step, config)
	//fields
	as = &AgaveSender{
		items:         items,
		graphName:     graphname,
		graphTemplate: graphtemplate,
		hosts:         hosts,
		logger:        logger,
		fields:        fields,
		step:          step,
	}
	return
}
