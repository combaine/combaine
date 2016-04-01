package solomon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
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
	SolomonHTTPClient = httpclient.NewClientWithTimeout(
		time.Millisecond*CONNECTION_TIMEOUT,
		time.Millisecond*RW_TIMEOUT)
)

const (
	connectionTimeout  = 900      //msec
	connectionEndpoint = ":42000" //msec
)

type SolomonSender interface {
	Send(tasks.DataType, uint64) error
}

type solomonClient struct {
	id      string
	api     string
	project string
	cluster string
	fields  []string
}

type SolomonCfg struct {
	Api     string   `codec:"api"`
	Project string   `codec:"project"`
	Cluster string   `codec:"cluster"`
	Fields  []string `codec:"Fields"`
}

type comLabels struct {
	Project string `json:"project"`
	Cluster string `json:"cluster"`
	Host    string `json:"host"`
	Service string `json:"service"`
}

type sensor struct {
	Labels map[string]string `json:"labels"`
	Ts     uint64            `json:"ts"`
	Value  float64           `json:"value"`
}

type solomonPush struct {
	CommonLabels comLabels `json:"commonLabels"`
	Sensors      []sensor  `json:"sensors"`
}

func (s *solomonClient) dumpSensor(sensors *[]sensor, name string,
	value reflect.Value, timestamp uint64) error {
	var (
		err         error
		sensorValue float64
	)

	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		sensorValue = value.Float()
	case reflect.Int:
		sensorValue = float64(value.Int())
	case reflect.String:
		sensorValue, err = strconv.ParseFloat(value.String(), 64)
	default:
		logger.Errf("Default case:, %t, %s:%s", value, value.Kind(), value)
		err = errors.New("Sensor is Not a Number")
	}
	if err != nil {
		return errors.New(fmt.Sprintf(
			"%s %s: %s", s.id, err, common.InterfaceToString(value)))
	}

	*sensors = append(*sensors, sensor{
		Labels: map[string]string{"sensor": name},
		Ts:     timestamp,
		Value:  sensorValue,
	})
	return nil
}

func (s *solomonClient) dumpSlice(sensors *[]sensor, name string,
	rv reflect.Value, timestamp uint64) error {

	if len(s.fields) == 0 || len(s.fields) != rv.Len() {
		msg := fmt.Sprintf("%s Unable to send a slice. Fields len %d, len of value %d",
			s.id, len(s.fields), rv.Len())
		logger.Errf("%s", msg)
		return errors.New(msg)
	}

	for i := 0; i < rv.Len(); i++ {
		sensorName := fmt.Sprintf("%s.%s", name, s.fields[i])
		item := reflect.ValueOf(rv.Index(i).Interface())
		err := s.dumpSensor(sensors, sensorName, item, timestamp)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *solomonClient) dumpMap(sensors *[]sensor, name string,
	rv reflect.Value, timestamp uint64) (err error) {

	keys := rv.MapKeys()
	for _, key := range keys {

		delimiter := "."
		if name == "" {
			delimiter = ""
		}
		sensorName := fmt.Sprintf("%s%s%s", name, delimiter, key)
		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		logger.Debugf("%s Item of key %s is: %v", s.id, key, itemInterface.Kind())

		switch itemInterface.Kind() {
		case reflect.Slice, reflect.Array:
			err = s.dumpSlice(sensors, sensorName, itemInterface, timestamp)
			if err != nil {
				return err
			}

		case reflect.Map:
			err = s.dumpMap(sensors, sensorName, itemInterface, timestamp)
			if err != nil {
				return err
			}

		default:
			err = s.dumpSensor(sensors, sensorName, itemInterface, timestamp)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *solomonClient) sendInternal(data *tasks.DataType, timestamp uint64) ([]solomonPush, error) {

	var (
		err      error
		pushData solomonPush
	)

	groups := make([]solomonPush, 0)

	for aggname, subgroupsAndValues := range *data {
		logger.Debugf("%s Sender handle aggregate named %s", s.id, aggname)

		for host, value := range subgroupsAndValues {
			sensors := make([]sensor, 0)
			pushData = solomonPush{
				CommonLabels: comLabels{
					Host:    host,
					Service: aggname,
					Cluster: s.cluster,
					Project: s.project,
				},
			}

			rv := reflect.ValueOf(value)
			logger.Debugf("%s %s", s.id, rv.Kind())

			if rv.Kind() == reflect.Map {
				err = s.dumpMap(&sensors, "", rv, timestamp)
				if err == nil {
					pushData.Sensors = sensors
					groups = append(groups, pushData)
				}
			} else {
				err = errors.New(fmt.Sprintf("%s Value of group should be dict", s.id))
			}
			if err != nil {
				logger.Warnf("%s bad value for %s: %s",
					s.id, aggname, common.InterfaceToString(value))
			}
		}
	}
	return groups, err
}

func (s *solomonClient) Send(task tasks.DataType, timestamp uint64) error {
	if len(task) == 0 {
		return fmt.Errorf("%s Empty data. Nothing to send.", s.id)
	}

	data, err := s.sendInternal(&task, timestamp)
	if err != nil {
		return fmt.Errorf("%s %s", s.id, err)
	}
	for _, v := range data {
		push, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("%s %s", s.id, err)
		}
		logger.Debugf("%s Data to POST: %s", s.id, string(push))

		req, err := http.NewRequest("POST", s.api, bytes.NewBuffer(push))
		if err != nil {
			return fmt.Errorf("%s %s", s.id, err)
		}
		req.Header.Add("Content-Type", "application/json")

		resp, err := SolomonHTTPClient.Do(req)
		if err != nil {
			return fmt.Errorf("%s %s", s.id, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("%s bad response code '%d': %s", s.id, resp.StatusCode, resp.Status)
			}
			return fmt.Errorf("%s bad response code '%d' '%s': %s", s.id, resp.StatusCode, resp.Status, b)
		}
	}

	return nil
}

func NewSolomonClient(cfg *SolomonCfg, id string) (ss SolomonSender, err error) {
	ss = &solomonClient{
		id:      id,
		api:     cfg.Api,
		cluster: cfg.Cluster,
		project: cfg.Project,
		fields:  cfg.Fields,
	}

	return
}
