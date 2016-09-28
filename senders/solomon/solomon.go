package solomon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
)

var (
	MaxWorkers  = 5
	PostRetries = 3
	JobQueue    = make(chan Job, MaxWorkers)
)

type SolomonSender interface {
	Send(tasks.DataType, uint64) error
}

type solomonClient struct {
	id                 string
	api                string
	prefix             string
	project            string
	cluster            string
	connection_timeout int
	rw_timeout         int
	fields             []string
}

type SolomonCfg struct {
	Api                string   `codec:"api"`
	Project            string   `codec:"project"`
	Cluster            string   `codec:"cluster"`
	Connection_timeout int      `codec:"connection_timeout"`
	Rw_timeout         int      `codec:"rw_timeout"`
	Fields             []string `codec:"Fields"`
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

type Job struct {
	PushData []byte
	SolCli   *solomonClient
}

type Worker struct {
	Id         int
	Retry      int
	JobChannel chan Job
}

type Dispatcher struct {
	WorkerPool chan chan Job
	Retry      int
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
		Labels: map[string]string{"sensor": s.prefix + name},
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

		service := aggname
		if strings.ContainsRune(aggname, '.') {
			aPrefix := strings.SplitN(aggname, ".", 2)
			service = aPrefix[0]
			s.prefix = aPrefix[1] + "."
		}

		for host, value := range subgroupsAndValues {
			sensors := make([]sensor, 0)
			pushData = solomonPush{
				CommonLabels: comLabels{
					Host:    host,
					Service: service,
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
	logger.Infof("%s Send %d items", s.id, len(data))
	for _, v := range data {
		push, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("%s %s", s.id, err)
		}
		logger.Debugf("%s Data to POST: %s", s.id, string(push))

		logger.Debugf("%s Sending work to JobQueue", s.id)
		JobQueue <- Job{PushData: push, SolCli: s}

	}

	return nil
}

func NewSolomonClient(cfg *SolomonCfg, id string) (ss SolomonSender, err error) {
	ss = &solomonClient{
		id:                 id,
		api:                cfg.Api,
		cluster:            cfg.Cluster,
		project:            cfg.Project,
		fields:             cfg.Fields,
		connection_timeout: cfg.Connection_timeout,
		rw_timeout:         cfg.Rw_timeout,
	}

	return
}

func (w Worker) Start() {
	for job := range JobQueue {
		logger.Debugf("%s worker %d received job", job.SolCli.id, w.Id)
		if err := w.SendToSolomon(job); err != nil {
			logger.Errf("%s", err)
		}
	}
}

func (w Worker) SendToSolomon(job Job) error {
	for i := 1; i <= w.Retry; i++ {
		var netErr net.Error
		var hasNetErr bool
		SolomonHTTPClient := httpclient.NewClientWithTimeout(
			time.Millisecond*(time.Duration(job.SolCli.connection_timeout)),
			time.Millisecond*(time.Duration(job.SolCli.rw_timeout)))
		req, err := http.NewRequest("POST", job.SolCli.api, bytes.NewReader(job.PushData))
		if err != nil {
			return fmt.Errorf("%s %s", job.SolCli.id, err)
		}
		req.Header.Add("Content-Type", "application/json")
		logger.Debugf("%s Attempting to send. Worker %d. Attempt %d", job.SolCli.id, w.Id, i)
		resp, err := SolomonHTTPClient.Do(req)

		if err != nil {
			netErr, hasNetErr = err.(net.Error)
			if hasNetErr == false {
				return fmt.Errorf("%s %s", job.SolCli.id, err)
			}
		}

		if err == nil {
			defer resp.Body.Close()
		}

		if hasNetErr && netErr.Timeout() || resp.StatusCode == http.StatusRequestTimeout {
			if i == w.Retry {
				return fmt.Errorf("%s failed to send after %d attemps. Worker %d. Dropping", job.SolCli.id, i, w.Id)
			}
			logger.Debugf("%s timed out. Worker %d. Retrying.", job.SolCli.id, w.Id)
			time.Sleep(time.Millisecond * 300)
			if err == nil {
				resp.Body.Close()
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("%s bad response code '%d': %s", job.SolCli.id, resp.StatusCode, resp.Status)
			}
			return fmt.Errorf("%s bad response code '%d' '%s': %s", job.SolCli.id, resp.StatusCode, resp.Status, b)
		}
		logger.Infof("%s Worker %d successfully sent data in %d attempts", job.SolCli.id, w.Id, i)
		break
	}
	return nil
}

func NewWorker(id int, retry int) Worker {
	return Worker{
		Id:    id,
		Retry: retry}
}

func StartWorkers() {
	for i := 0; i < cap(JobQueue); i++ {
		logger.Debugf("Creating worker %d", i)
		worker := NewWorker(i, PostRetries)
		go worker.Start()
	}
}
