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

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
)

var (
	postRetries = 3
	// JobQueue is channel from with workers consume jobs
	JobQueue = make(chan Job, 5 /*max workers*/)
)

// Config contains setting from combainers sender section
type Config struct {
	API     string   `codec:"api"`
	Project string   `codec:"project"`
	Cluster string   `codec:"cluster"`
	Timeout int      `codec:"rw_timeout"`
	Fields  []string `codec:"Fields"`
}

// Sender object
type Sender struct {
	Config
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

func (s *Sender) dumpSensor(sensors *[]sensor, name string,
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
		logger.Errf("%s Default case:, %t, %s:%s", s.id, value, value.Kind(), value)
		err = fmt.Errorf("Sensor is Not a Number")
	}
	if err != nil {
		return fmt.Errorf("%s: %s", err, common.InterfaceToString(value))
	}

	*sensors = append(*sensors, sensor{
		Labels: map[string]string{"sensor": s.prefix + name},
		Ts:     timestamp,
		Value:  sensorValue,
	})
	return nil
}

func (s *Sender) dumpSlice(sensors *[]sensor, name string,
	rv reflect.Value, timestamp uint64) error {

	if len(s.Fields) == 0 || len(s.Fields) != rv.Len() {
		msg := fmt.Sprintf("%s Unable to send a slice. Fields len %d, len of value %d",
			s.id, len(s.Fields), rv.Len())
		logger.Errf("%s %s", s.id, msg)
		return fmt.Errorf(msg)
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

func (s *Sender) dumpMap(sensors *[]sensor, name string,
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
		case reflect.Map:
			err = s.dumpMap(sensors, sensorName, itemInterface, timestamp)
		default:
			err = s.dumpSensor(sensors, sensorName, itemInterface, timestamp)
		}

		if err != nil {
			return
		}
	}
	return
}

func (s *Sender) sendInternal(data []tasks.AggregationResult, timestamp uint64) ([]solomonPush, error) {

	var (
		host     string
		err      error
		pushData solomonPush
	)

	var groups []solomonPush

	//for aggname, subgroupsAndValues := range data {
	for _, item := range data {
		aggname := item.Tags["aggregate"]
		logger.Infof("%s Handle aggregate named %s", s.id, aggname)

		service := aggname
		if strings.ContainsRune(aggname, '.') {
			aPrefix := strings.SplitN(aggname, ".", 2)
			service = aPrefix[0]
			s.prefix = aPrefix[1] + "."
		}

		host, err = common.GetSubgroupName(item.Tags)
		if err != nil {
			logger.Errf("%s skip task: %s", s.id, err)
			continue
		}
		//for host, value := range subgroupsAndValues {
		var sensors []sensor
		pushData = solomonPush{
			CommonLabels: comLabels{
				Host:    host,
				Service: service,
				Cluster: s.Cluster,
				Project: s.Project,
			},
		}

		rv := reflect.ValueOf(item.Result)
		logger.Debugf("%s %s", s.id, rv.Kind())

		if rv.Kind() == reflect.Map {
			err = s.dumpMap(&sensors, "", rv, timestamp)
			if err == nil {
				pushData.Sensors = sensors
				groups = append(groups, pushData)
			}
		} else {
			err = fmt.Errorf("Value of group should be dict")
		}
		if err != nil {
			logger.Errf("%s bad value for %s - %s: %s", s.id, aggname, err, common.InterfaceToString(item.Result))
		}
	}
	return groups, err
}

// Send parse data, build and send http request to solomon api
func (s *Sender) Send(task []tasks.AggregationResult, timestamp uint64) error {
	if len(task) == 0 {
		return fmt.Errorf("Empty data. Nothing to send")
	}

	data, err := s.sendInternal(task, timestamp)
	if err != nil {
		return err
	}
	logger.Infof("%s Send %d items", s.id, len(data))
	for _, v := range data {
		push, err := json.Marshal(v)
		if err != nil {
			return err
		}
		logger.Debugf("%s Data to POST: %s", s.id, string(push))

		logger.Debugf("%s Sending work to JobQueue", s.id)
		JobQueue <- Job{PushData: push, SolCli: s}

	}
	return nil
}

// NewSender return new instance of solomon sender
func NewSender(config Config, id string) (*Sender, error) {
	return &Sender{Config: config, id: id}, nil
}

func (w Worker) sendToSolomon(job Job) error {
	var sendErr error
	var attempt = 0
	var isTimeout bool

	for {
		if attempt >= w.Retry {
			sendErr = fmt.Errorf("%s worker %d failed to send after %d attempts, dropping job", job.SolCli.id, w.id, attempt)
			break
		}
		attempt++

		logger.Debugf("%s attempting to send. Worker %d. Attempt %d", job.SolCli.id, w.id, attempt)
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(job.SolCli.Timeout)*time.Millisecond)
		resp, err := httpclient.Post(ctx, job.SolCli.API, "application/json", bytes.NewReader(job.PushData))
		cancel()
		switch err {
		case nil:
			// The default HTTP client's Transport does not attempt to
			// reuse HTTP/1.0 or HTTP/1.1 TCP connections ("keep-alive")
			// unless the Body is read to completion and is closed.
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			// err is nil and there may occure some http errors includeing timeout
			if resp.StatusCode == http.StatusOK {
				sendErr = nil
				logger.Infof("%s worker %d successfully sent data in %d attempts", job.SolCli.id, w.id, attempt)
				break
			}
			sendErr = fmt.Errorf("bad status='%d %s', response: %s", resp.StatusCode, resp.Status, b)
			isTimeout = resp.StatusCode == http.StatusRequestTimeout
		case context.Canceled, context.DeadlineExceeded:
			isTimeout = true
		default:
			sendErr = fmt.Errorf("send error: %s", err)
		}

		if !isTimeout {
			// do not retry on net or http non timeout errors
			break
		}

		logger.Debugf("%s timed out. Worker %d. Retrying", job.SolCli.id, w.id)
		if attempt < w.Retry {
			time.Sleep(time.Millisecond * w.RetryInterval)
		}
	}
	return sendErr
}

func (w Worker) start(j chan Job) {
	for job := range j {
		logger.Debugf("%s worker %d received job", job.SolCli.id, w.id)
		if err := w.sendToSolomon(job); err != nil {
			logger.Errf("%s %s", job.SolCli.id, err)
		}
	}
}

func newWorker(id int, retry int, interval int) Worker {
	return Worker{id: id, Retry: retry, RetryInterval: time.Duration(interval)}
}

// StartWorkers run predefined number of workers,
// workers concurently wite job from JobQueue
func StartWorkers(j chan Job, retryInterval int) {
	for i := 0; i < cap(j); i++ {
		logger.Debugf("Creating worker %d", i)
		worker := newWorker(i, postRetries, retryInterval)
		go worker.start(j)
	}
}
