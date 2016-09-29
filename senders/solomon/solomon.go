package solomon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context/ctxhttp"

	"github.com/combaine/combaine/common"
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

type SolomonCfg struct {
	Api     string   `codec:"api"`
	Project string   `codec:"project"`
	Cluster string   `codec:"cluster"`
	Timeout int      `codec:"rw_timeout"`
	Fields  []string `codec:"Fields"`
}

type solomonClient struct {
	SolomonCfg
	id     string
	prefix string
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
	Id    int
	Retry int
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
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		sensorValue = float64(value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		sensorValue = float64(value.Uint())
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

	if len(s.Fields) == 0 || len(s.Fields) != rv.Len() {
		msg := fmt.Sprintf("%s Unable to send a slice. Fields len %d, len of value %d",
			s.id, len(s.Fields), rv.Len())
		logger.Errf("%s", msg)
		return errors.New(msg)
	}

	for i := 0; i < rv.Len(); i++ {
		sensorName := fmt.Sprintf("%s.%s", name, s.Fields[i])
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
					Cluster: s.Cluster,
					Project: s.Project,
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
		push, _ := json.Marshal(v)
		logger.Debugf("%s Data to POST: %s", s.id, string(push))

		logger.Debugf("%s Sending work to JobQueue", s.id)
		JobQueue <- Job{PushData: push, SolCli: s}

	}
	return nil
}

func NewSolomonClient(cfg SolomonCfg, id string) (SolomonSender, error) {
	return &solomonClient{SolomonCfg: cfg, id: id}, nil
}

func (w Worker) SendToSolomon(job Job) error {
	var sendErr error
	var attempt = 0
	var isTimeout bool

	for {
		if attempt >= w.Retry {
			sendErr = fmt.Errorf("%s worker %d failed to send after %d attempts, dropping job", job.SolCli.id, w.Id, attempt)
			break
		}
		attempt++

		req, err := http.NewRequest("POST", job.SolCli.Api, bytes.NewReader(job.PushData))
		if err != nil {
			sendErr = fmt.Errorf("%s request error: %s", job.SolCli.id, err)
			break
		}
		req.Header.Add("Content-Type", "application/json")

		logger.Debugf("%s attempting to send. Worker %d. Attempt %d", job.SolCli.id, w.Id, attempt)
		ctx, cancelFunc := context.WithTimeout(context.TODO(), time.Duration(job.SolCli.Timeout)*time.Millisecond)
		defer cancelFunc()
		resp, err := ctxhttp.Do(ctx, nil, req)
		switch err {
		case nil:
			// err is nil and there may occure some http errors includeing timeout
			if resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				sendErr = nil
				logger.Infof("%s worker %d successfully sent data in %d attempts", job.SolCli.id, w.Id, attempt)
				break
			}
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			sendErr = fmt.Errorf("%s bad status='%d %s', response: %s", job.SolCli.id, resp.StatusCode, resp.Status, b)
			isTimeout = resp.StatusCode == http.StatusRequestTimeout
		case context.Canceled, context.DeadlineExceeded:
			isTimeout = true
		default:
			sendErr = fmt.Errorf("%s send error: %s", job.SolCli.id, err)
		}

		if !isTimeout {
			// do not retry on net or http non timeout errors
			break
		}

		logger.Debugf("%s timed out. Worker %d. Retrying.", job.SolCli.id, w.Id)
		if attempt < w.Retry {
			time.Sleep(time.Millisecond * 300)
		}
	}
	return sendErr
}

func (w Worker) Start(j chan Job) {
	for job := range j {
		logger.Debugf("%s worker %d received job", job.SolCli.id, w.Id)
		if err := w.SendToSolomon(job); err != nil {
			logger.Errf("%s", err)
		}
	}
}

func NewWorker(id int, retry int) Worker {
	return Worker{Id: id, Retry: retry}
}

func StartWorkers(j chan Job) {
	for i := 0; i < cap(j); i++ {
		logger.Debugf("creating worker %d", i)
		worker := NewWorker(i, PostRetries)
		go worker.Start(j)
	}
}
