package razladki

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
)

const (
	CONNECTION_TIMEOUT = 2000 // ms
	RW_TIMEOUT         = 3000 // ms
)

var (
	RazladkiHttpClient = httpclient.NewClientWithTimeout(
		time.Millisecond*CONNECTION_TIMEOUT,
		time.Millisecond*RW_TIMEOUT)
)

type RazladkiConfig struct {
	Items   map[string]string `codec:"items"`
	Project string            `codec:"project"`
	Host    string            `codec:"host"`
}

type RazladkiSender struct {
	*RazladkiConfig
	id string
}

type Meta struct {
	Title string `json:"title"`
}

type Param struct {
	Value string `json:"value"`
	Meta  Meta
}

type Alarm struct {
	Meta Meta
}

type RazladkiResult struct {
	Timestamp uint64           `json:"ts"`
	Params    map[string]Param `json:"params"`
	Alarms    map[string]Alarm `json:"alarms"`
}

func (r *RazladkiResult) Push(name, value, title string) {
	r.Params[fmt.Sprintf(name)] = Param{
		Value: value,
		Meta: Meta{
			Title: title,
		},
	}
	r.Alarms[name] = Alarm{
		Meta: Meta{
			Title: title,
		},
	}
}

func NewRazladkiClient(cfg *RazladkiConfig, id string) (*RazladkiSender, error) {
	return &RazladkiSender{
		RazladkiConfig: cfg,
		id:             id,
	}, nil
}

func (r *RazladkiSender) send(data tasks.DataType, timestamp uint64) (*RazladkiResult, error) {
	logger.Debugf("%s Data to send: %v", r.id, data)
	result := RazladkiResult{
		Timestamp: timestamp,
		Params:    make(map[string]Param),
		Alarms:    make(map[string]Alarm),
	}
	for aggname, title := range r.Items {
		var root, metricname string
		items := strings.SplitN(aggname, ".", 2)
		if len(items) > 1 {
			root, metricname = items[0], items[1]
		} else {
			root = items[0]
		}
		for subgroup, value := range data[root] {
			rv := reflect.ValueOf(value)
			switch rv.Kind() {
			case reflect.Slice, reflect.Array:
				// unsupported
			case reflect.Map:
				if len(metricname) == 0 {
					continue
				}

				key := reflect.ValueOf(metricname)
				mapVal := rv.MapIndex(key)
				if !mapVal.IsValid() {
					continue
				}

				value := reflect.ValueOf(mapVal.Interface())

				switch value.Kind() {
				case reflect.Slice, reflect.Array:
					// unsupported
				case reflect.Map:
					// unsupported
				default:
					name := fmt.Sprintf("%s_%s", subgroup, metricname)
					result.Push(name, common.InterfaceToString(value.Interface()), title)
				}
			default:
				if len(metricname) != 0 {
					continue
				}
				name := fmt.Sprintf("%s_%s", subgroup, root)
				result.Push(name, common.InterfaceToString(value), title)
			}
		}
	}
	return &result, nil
}

func (r *RazladkiSender) Send(data tasks.DataType, timestamp uint64) error {
	res, err := r.send(data, timestamp)
	if err != nil {
		return err
	}

	var buffer = new(bytes.Buffer)
	if err = json.NewEncoder(buffer).Encode(res); err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/save_new_data_json/%s", r.Host, r.Project)
	logger.Infof("%s send to url %s, data %s", r.id, url, buffer.Bytes())
	req, err := http.NewRequest("POST", url, buffer)
	if err != nil {
		return err
	}

	resp, err := RazladkiHttpClient.Do(req)
	if err != nil {
		logger.Errf("%s unable to do http request: %v", r.id, err)
		return err
	}
	defer resp.Body.Close()

	logger.Infof("%s response status %d %s", r.id, resp.StatusCode, resp.Status)
	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("bad response code %d: %s", resp.StatusCode, resp.Status)
		}
		return fmt.Errorf("bad response code %d %s: %s", resp.StatusCode, resp.Status, b)
	}

	return nil
}
