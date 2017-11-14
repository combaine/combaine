package razladki

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"golang.org/x/net/context/ctxhttp"
)

// Config containse fields from compbainer task config
type Config struct {
	Items   map[string]string `codec:"items"`
	Project string            `codec:"project"`
	Host    string            `codec:"host"`
	Fields  []string          `codec:"Fields"`
}

// Sender main sender object
type Sender struct {
	*Config
	id string
}

// Meta field in razladki request json
type Meta struct {
	Title string `json:"title"`
}

// Param field in razladki request json
type Param struct {
	Value string `json:"value"`
	Meta  Meta
}

// Alarm field in razladki request json
type Alarm struct {
	Meta Meta
}

type result struct {
	Timestamp uint64           `json:"ts"`
	Params    map[string]Param `json:"params"`
	Alarms    map[string]Alarm `json:"alarms"`
}

func (r *result) Push(name, value, title string) {
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

// InitializeLogger create cocaine logger
func InitializeLogger(init func()) { init() }

// NewSender build new razladki sender
func NewSender(cfg *Config, id string) (*Sender, error) {
	return &Sender{
		Config: cfg,
		id:     id,
	}, nil
}

func (r *Sender) send(data []common.AggregationResult, timestamp uint64) (*result, error) {
	logger.Debugf("%s Data to send: %v", r.id, data)
	res := result{
		Timestamp: timestamp,
		Params:    make(map[string]Param),
		Alarms:    make(map[string]Alarm),
	}

	var queryItems = make(map[string]map[string]string)
	for aggname, title := range r.Items {
		items := strings.SplitN(aggname, ":", 2)
		mName := ""
		if len(items) > 1 {
			mName = items[1]
		}
		if mp, ok := queryItems[items[0]]; !ok {
			queryItems[items[0]] = map[string]string{mName: title}
		} else {
			mp[mName] = title
		}
	}

	name, arrayIdxKey := "", ""
	for _, item := range data {
		var root string
		var metrics map[string]string
		var ok bool

		if root, ok = item.Tags["aggregate"]; !ok {
			logger.Errf("%s Failed to get data tag 'aggregate', skip task: %v", r.id, item)
			continue
		}
		if metrics, ok = queryItems[root]; !ok {
			logger.Debugf("%s %s not in Items, skip task: %v", r.id, root, item)
			continue
		}
		subgroup, err := common.GetSubgroupName(item.Tags)
		if err != nil {
			logger.Errf("%s %s", r.id, err)
			continue
		}

		rv := reflect.ValueOf(item.Result)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			// unsupported
		case reflect.Map:
			if len(metrics) == 0 {
				continue
			}

			for keyStr, title := range metrics {
				arrayIdx := -1
				key := reflect.ValueOf(keyStr)
				if strings.IndexByte(keyStr, '[') > 0 && strings.IndexByte(keyStr, ']') == len(keyStr)-1 {
					splitRes := strings.Split(keyStr, "[")
					name, arrayIdxKey = splitRes[0], splitRes[1]
					key = reflect.ValueOf(name)
					arrayIdxKey = strings.Trim(arrayIdxKey, "]'\"")
					arrayIdx, err = strconv.Atoi(arrayIdxKey)
					if err != nil || arrayIdx == -1 {
						logger.Errf("%s failed to extract index from %s: %v", r.id, keyStr, err)
						continue
					}
				}
				mapVal := rv.MapIndex(key)
				if !mapVal.IsValid() {
					continue
				}

				value := reflect.ValueOf(mapVal.Interface())

				switch value.Kind() {
				case reflect.Slice, reflect.Array:
					if arrayIdx >= value.Len() {
						logger.Errf("%s idx %d out of range %d:%v", r.id, arrayIdx, value.Len(), value)
						continue
					}
					if len(r.Fields) == 0 || len(r.Fields) != value.Len() {
						logger.Errf("%s Fields len %d, not match value len %d", r.id, len(r.Fields), value.Len())
						continue
					}
					name = fmt.Sprintf("%s_%s.%s", subgroup, name, r.Fields[arrayIdx])
					value = value.Index(arrayIdx)
					if !value.IsValid() {
						logger.Errf("%s Failed to extract value at %d from %v", r.id, arrayIdx, value)
						continue
					}
					res.Push(name, common.InterfaceToString(value.Interface()), title)

				case reflect.Map:
					// unsupported
				default:
					name = fmt.Sprintf("%s_%s", subgroup, key)
					res.Push(name, common.InterfaceToString(value.Interface()), title)
				}
			}
		default:
			if title, ok := metrics[""]; ok {
				name = fmt.Sprintf("%s_%s", subgroup, root)
				res.Push(name, common.InterfaceToString(item.Result), title)
			}
		}
	}
	return &res, nil
}

// Send perform request to razladki service
func (r *Sender) Send(ctx context.Context, data []common.AggregationResult, timestamp uint64) error {
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

	resp, err := ctxhttp.Do(ctx, nil, req)
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
