package graphite

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/combaine/combaine/senders"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

const (
	onePointFormat = "%s.combaine.%s.%s %s %d\n"
)

// should be var for testing purposes
var (
	connectionTimeout = 300 //msec
	reconnectInterval = 50  //msec
)

// Sender contains main setting for graphite sender
type Sender struct {
	id       string
	cluster  string
	endpoint string
	fields   []string
}

// Config extract sender config from task data
type Config struct {
	Cluster  string   `codec:"cluster"`
	Endpoint string   `codec:"endpoint"`
	Fields   []string `codec:"Fields"`
}

type pointFormat func(utils.NameStack, interface{}, int64) string

func makePoint(format, cluster, subgroup string) pointFormat {
	return func(metric utils.NameStack, value interface{}, timestamp int64) string {
		return fmt.Sprintf(
			format,
			cluster,
			formatSubgroup(subgroup),
			strings.Join(metric, "."),
			utils.InterfaceToString(value),
			timestamp,
		)
	}
}

func formatSubgroup(input string) string {
	return strings.Replace(strings.Replace(input, ".", "_", -1), "-", "_", -1)
}

func (g *Sender) send(output io.Writer, data string) error {
	logrus.Debugf("%s Send %s", g.id, data)
	if _, err := fmt.Fprint(output, data); err != nil {
		logrus.Errorf("%s Sending error: %s", g.id, err)
		connPool.Evict(output)
		return err
	}
	return nil
}

func (g *Sender) sendInterface(output io.Writer, metricName utils.NameStack,
	f pointFormat, value interface{}, timestamp int64) error {
	data := f(metricName, value, timestamp)
	return g.send(output, data)
}

func (g *Sender) sendSlice(output io.Writer, metricName utils.NameStack, f pointFormat,
	rv reflect.Value, timestamp int64) error {

	if len(g.fields) == 0 || len(g.fields) != rv.Len() {
		return fmt.Errorf("%s Unable to send a slice. Fields len %d, len of value %d",
			g.id, len(g.fields), rv.Len())
	}

	for i := 0; i < rv.Len(); i++ {
		metricName.Push(g.fields[i])

		item := rv.Index(i).Interface()
		err := g.sendInterface(output, metricName, f, utils.InterfaceToString(item), timestamp)
		if err != nil {
			return err
		}

		metricName.Pop()
	}
	return nil
}

func (g *Sender) sendMap(output io.Writer, metricName utils.NameStack, f pointFormat,
	rv reflect.Value, timestamp int64) (err error) {

	keys := rv.MapKeys()
	for _, key := range keys {
		// Push key of map
		metricName.Push(utils.InterfaceToString(key.Interface()))

		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		logrus.Debugf("%s Item of key %s is: %v", g.id, key, itemInterface.Kind())

		switch itemInterface.Kind() {
		case reflect.Slice, reflect.Array:
			err = g.sendSlice(output, metricName, f, itemInterface, timestamp)
		case reflect.Map:
			err = g.sendMap(output, metricName, f, itemInterface, timestamp)
		default:
			err = g.sendInterface(output, metricName, f,
				utils.InterfaceToString(itemInterface.Interface()), timestamp)
		}
		if err != nil {
			break
		}
		// Pop key of map
		metricName.Pop()
	}
	return
}

func (g *Sender) sendInternal(data []*senders.Payload, timestamp int64, output io.Writer) (err error) {
	metricName := make(utils.NameStack, 0, 3)

	logrus.Infof("%s send %d aggregates", g.id, len(data))
	for _, aggItem := range data {
		aggname := aggItem.Tags["aggregate"]

		metricName.Push(aggname)
		subgroup, err := utils.GetSubgroupName(aggItem.Tags)
		if err != nil {
			logrus.Errorf("%s %s", g.id, err)
			continue
		}
		pointFormatter := makePoint(onePointFormat, g.cluster, subgroup)
		rv := reflect.ValueOf(aggItem.Result)
		logrus.Debugf("%s data kind '%s' for aggregate %s", g.id, rv.Kind(), aggname)

		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			err = g.sendSlice(output, metricName, pointFormatter, rv, timestamp)
		case reflect.Map:
			err = g.sendMap(output, metricName, pointFormatter, rv, timestamp)
		default:
			err = g.sendInterface(output, metricName, pointFormatter, aggItem.Result, timestamp)
		}
		if err != nil {
			return err
		}
		metricName.Pop()
	}
	return
}

// Send proxy send operation to all graphite endpoints
func (g *Sender) Send(data []*senders.Payload, timestamp int64) error {
	if len(data) == 0 {
		return fmt.Errorf("%s Empty data. Nothing to send", g.id)
	}

	// TODO: make endpoints as slice, and send to all endpoints in parallel
	sock, err := connPool.Get(g.endpoint, 3, connectionTimeout)
	if err != nil {
		return err
	}

	return g.sendInternal(data, timestamp, sock)
}

// NewSender return pointer to sender with specified config
func NewSender(cfg *Config, id string) (gs *Sender, err error) {
	gs = &Sender{
		id:       id,
		cluster:  cfg.Cluster,
		fields:   cfg.Fields,
		endpoint: cfg.Endpoint,
	}
	return
}
