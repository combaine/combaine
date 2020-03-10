package solomon

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
	"time"

	"github.com/combaine/combaine/common/chttp"
	"github.com/combaine/combaine/senders"
	"github.com/combaine/combaine/utils"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	postRetries = 3
	// JobQueue is channel from with workers consume jobs
	JobQueue = make(chan Job, 5 /*max workers*/)
)

// Config contains setting from combainers sender section
type Config struct {
	API     string   `msgpack:"api"`
	Project string   `msgpack:"project"`
	Cluster string   `msgpack:"cluster"`
	Service string   `msgpack:"service"`
	Timeout int      `msgpack:"timeout"`
	Fields  []string `msgpack:"Fields"`
	Schema  []string `msgpack:"schema"`
}

// Sender object
type Sender struct {
	Config
	log    *logrus.Entry
	prefix string
}

type comLabels struct {
	Project string `json:"project"`
	Cluster string `json:"cluster"`
	Host    string `json:"host"`
	Service string `json:"service"`
}

// Sensor type
type Sensor struct {
	Labels map[string]string `json:"labels"`
	Ts     int64             `json:"ts"`
	Value  float64           `json:"value"`
}

type solomonPush struct {
	CommonLabels comLabels `json:"commonLabels"`
	Sensors      []Sensor  `json:"sensors"`
}

// Job contains sender and PushData
type Job struct {
	PushData []byte
	SolCli   *Sender
}

// Worker consume jobs from JobQueue and try run Send method of Senders
type Worker struct {
	id            int
	Retry         int           // count
	RetryInterval time.Duration // ms
}

func (s *Sender) dumpSensor(path utils.NameStack, value reflect.Value, timestamp int64) (*Sensor, error) {
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
		s.log.Errorf("Default case:, %v, %s:%s", value, value.Kind(), value)
		err = fmt.Errorf("Sensor is Not a Number")
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %v", err, value)
	}
	sensor := Sensor{
		Labels: make(map[string]string),
		Ts:     timestamp,
		Value:  sensorValue,
	}

	if len(s.Schema) > 0 {
		if len(path) == 1 {
			s.log.Infof("Handle graphite compatible sensor name %s by schema %+v", path, s.Schema)
			path = strings.Split(path[0], ".")
		}

		if len(s.Schema) >= len(path) {
			msg := fmt.Errorf("Unable to send %+v(size %d) with schema %+v(size %d)",
				path, len(path), s.Schema, len(s.Schema))
			s.log.Error(msg)
			return nil, msg
		}
		for i := 0; i < len(s.Schema); i++ {
			sensor.Labels[s.Schema[i]] = path[i]
		}
		sensor.Labels["sensor"] = strings.Join(path[len(s.Schema):], ".")
		if s.prefix != "" {
			sensor.Labels["prefix"] = strings.TrimRight(s.prefix, ".")
		}
	} else {
		sensor.Labels["sensor"] = s.prefix + strings.Join(path, ".")
	}

	return &sensor, nil
}

func (s *Sender) dumpSlice(sensors *[]Sensor, path utils.NameStack,
	rv reflect.Value, timestamp int64) error {

	if len(s.Fields) == 0 || len(s.Fields) != rv.Len() {
		msg := errors.Errorf("Unable to send a slice. Fields len %d, len of value %d", len(s.Fields), rv.Len())
		s.log.Error(msg)
		return msg
	}

	for i := 0; i < rv.Len(); i++ {
		data := reflect.ValueOf(rv.Index(i).Interface())
		path.Push(s.Fields[i])
		item, err := s.dumpSensor(path, data, timestamp)
		path.Pop()
		if err != nil {
			return err
		}
		*sensors = append(*sensors, *item)
	}
	return nil
}

func (s *Sender) dumpMap(sensors *[]Sensor, path utils.NameStack, rv reflect.Value, timestamp int64) error {
	var (
		item *Sensor
		err  error
	)

	for _, key := range rv.MapKeys() {
		path.Push(fmt.Sprintf("%s", key))
		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		s.log.Debugf("Item of key %s is: %v", key, itemInterface.Kind())

		switch itemInterface.Kind() {
		case reflect.Slice, reflect.Array:
			err = s.dumpSlice(sensors, path, itemInterface, timestamp)
		case reflect.Map:
			err = s.dumpMap(sensors, path, itemInterface, timestamp)
		default:
			item, err = s.dumpSensor(path, itemInterface, timestamp)
			if err == nil {
				*sensors = append(*sensors, *item)
			}
		}

		if err != nil {
			return err
		}
		path.Pop()
	}
	return err
}

func (s *Sender) sendInternal(data []*senders.Payload, timestamp int64) ([]solomonPush, error) {
	var (
		groups []solomonPush
		err    error
		host   string
	)

	//for aggname, subgroupsAndValues := range data {
	for _, item := range data {
		aggname := item.Tags["aggregate"]
		s.log.Infof("Handle aggregate named %s", aggname)

		service := s.Service
		if service == "" {
			service = aggname
		}
		if strings.ContainsRune(service, '.') {
			aPrefix := strings.SplitN(service, ".", 2)
			service = aPrefix[0]
			s.prefix = aPrefix[1] + "."
		}

		host, err = utils.GetSubgroupName(item.Tags)
		if err != nil {
			err = fmt.Errorf("skip task: %s", err)
			continue
		}
		//for host, value := range subgroupsAndValues {
		var sensors []Sensor
		pushData := solomonPush{
			CommonLabels: comLabels{
				Host:    host,
				Service: service,
				Cluster: s.Cluster,
				Project: s.Project,
			},
		}

		rv := reflect.ValueOf(item.Result)
		s.log.Debugf("%s", rv.Kind())

		if rv.Kind() != reflect.Map {
			err = fmt.Errorf("Value of group should be dict, skip: %v", rv)
			continue
		}
		err = s.dumpMap(&sensors, []string{}, rv, timestamp)
		if err != nil {
			err = fmt.Errorf("bad value for %s - %s: %v", aggname, err, item.Result)
			continue
		}
		if len(sensors) == 0 {
			s.log.Infof("Empty sensors for %v", pushData)
			continue
		}
		pushData.Sensors = sensors
		groups = append(groups, pushData)
	}
	return groups, err
}

// Send parse data, build and send http request to solomon api
func (s *Sender) Send(task []*senders.Payload, timestamp int64) error {
	if len(task) == 0 {
		s.log.Warn("Empty data. Nothing to send")
		return nil
	}

	data, err := s.sendInternal(task, timestamp)
	if err != nil {
		return err
	}
	s.log.Infof("Send %d items", len(data))
	var merr *multierror.Error
	for _, v := range data {
		push, err := json.Marshal(v)
		if err != nil {
			merr = multierror.Append(merr, err)
		}
		s.log.Debugf("Sending work to JobQueue, Data to POST: %s", string(push))
		JobQueue <- Job{PushData: push, SolCli: s}

	}
	return merr.ErrorOrNil()
}

// NewSender return new instance of solomon sender
func NewSender(config Config, log *logrus.Entry) (*Sender, error) {
	return &Sender{Config: config, log: log}, nil
}

func (w Worker) sendToSolomon(job Job) error {
	var sendErr error
	var attempt = 0
	var isTimeout bool

	for {
		if attempt >= w.Retry {
			sendErr = errors.Errorf("worker %d failed to send after %d attempts, dropping job", w.id, attempt)
			break
		}
		attempt++

		job.SolCli.log.Debugf("attempting to send. Worker %d. Attempt %d", w.id, attempt)
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(job.SolCli.Timeout)*time.Millisecond)
		resp, err := chttp.Post(ctx, job.SolCli.API, "application/json", bytes.NewReader(job.PushData))
		cancel()
		switch err {
		case nil:
			// The default HTTP client's Transport does not attempt to
			// reuse HTTP/1.0 or HTTP/1.1 TCP connections ("keep-alive")
			// unless the Body is read to completion and is closed.
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			// err is nil and there may occure some http errors including timeout
			if resp.StatusCode == http.StatusOK {
				sendErr = nil
				job.SolCli.log.Infof("worker %d successfully sent data in %d attempts", w.id, attempt)
				break
			}
			sendErr = fmt.Errorf("bad status='%d %s', response: %s", resp.StatusCode, resp.Status, b)
			isTimeout = resp.StatusCode == http.StatusRequestTimeout
		case context.Canceled, context.DeadlineExceeded:
			isTimeout = true
		default:
			sendErr = fmt.Errorf("Sending error: %s", err)
		}

		if !isTimeout {
			// do not retry on net or http non timeout errors
			break
		}

		job.SolCli.log.Debugf("timed out. Worker %d. Retrying", w.id)
		if attempt < w.Retry {
			time.Sleep(time.Millisecond * w.RetryInterval)
		}
	}
	return sendErr
}

func (w Worker) start(j chan Job) {
	for job := range j {
		job.SolCli.log.Debugf("worker %d received job", w.id)
		if err := w.sendToSolomon(job); err != nil {
			job.SolCli.log.Error(err)
		}
	}
}

func newWorker(id int, retry int, interval int) Worker {
	return Worker{id: id, Retry: retry, RetryInterval: time.Duration(interval)}
}

// StartWorkers run predefined number of workers,
// workers wait on channel JobQueue
func StartWorkers(j chan Job, retryInterval int) {
	for i := 0; i < cap(j); i++ {
		logrus.Debugf("Creating worker %d", i)
		worker := newWorker(i, postRetries, retryInterval)
		go worker.start(j)
	}
}
