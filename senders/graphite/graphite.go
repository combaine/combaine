package graphite

import (
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/combaine/combaine/senders"
	"github.com/combaine/combaine/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// should be var for testing purposes
var (
	connectionTimeout = 300 //msec
	reconnectInterval = 50  //msec
)

// Sender contains main setting for graphite sender
type Sender struct {
	log      *logrus.Entry
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

type pointFormat func(utils.NameStack, string, int64) string

func makePoint(cluster, subgroup string) pointFormat {
	subgroup = formatSubgroup(subgroup)
	return func(metric utils.NameStack, value string, timestamp int64) string {
		return cluster +
			".combaine" +
			"." + subgroup +
			"." + strings.Join(metric, ".") +
			" " + value +
			" " + strconv.FormatInt(timestamp, 10) + "\n"
	}
}

func formatSubgroup(input string) string {
	return strings.Replace(strings.Replace(input, ".", "_", -1), "-", "_", -1)
}

func (g *Sender) send(output io.Writer, data string) error {
	g.log.Debugf("Send %q", data)
	if _, err := io.WriteString(output, data); err != nil {
		g.log.Errorf("Sending error: %s", err)
		connPool.Evict(output)
		return err
	}
	return nil
}

func (g *Sender) sendInterface(output io.Writer, metricName utils.NameStack, f pointFormat, value string, timestamp int64) error {
	data := f(metricName, value, timestamp)
	return g.send(output, data)
}

func (g *Sender) sendSlice(output io.Writer, metricName utils.NameStack, f pointFormat, rv reflect.Value, timestamp int64) error {

	if len(g.fields) == 0 || len(g.fields) != rv.Len() {
		return errors.Errorf("Unable to send a slice. Fields len %d, len of value %d", len(g.fields), rv.Len())
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

func (g *Sender) sendMap(output io.Writer, metricName utils.NameStack, f pointFormat, rv reflect.Value, timestamp int64) (err error) {

	keys := rv.MapKeys()
	for _, key := range keys {
		// Push key of map
		metricName.Push(utils.InterfaceToString(key.Interface()))

		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		g.log.Debugf("Item of key %s is: %v", key, itemInterface.Kind())

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

	g.log.Infof("send %d aggregates", len(data))
	for _, aggItem := range data {
		aggname := aggItem.Tags["aggregate"]

		metricName.Push(aggname)
		subgroup, err := utils.GetSubgroupName(aggItem.Tags)
		if err != nil {
			g.log.Error(err)
			continue
		}
		pointFormatter := makePoint(g.cluster, subgroup)
		rv := reflect.ValueOf(aggItem.Result)
		g.log.Debugf("data kind '%s' for aggregate %s", rv.Kind(), aggname)

		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			err = g.sendSlice(output, metricName, pointFormatter, rv, timestamp)
		case reflect.Map:
			err = g.sendMap(output, metricName, pointFormatter, rv, timestamp)
		default:
			err = g.sendInterface(output, metricName, pointFormatter, utils.InterfaceToString(aggItem.Result), timestamp)
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
		g.log.Warn("Empty data. Nothing to send")
		return nil
	}

	// TODO: make endpoints as slice, and send to all endpoints in parallel
	sock, err := connPool.Get(g.endpoint, 3, connectionTimeout)
	if err != nil {
		return err
	}

	return g.sendInternal(data, timestamp, sock)
}

// NewSender return pointer to sender with specified config
func NewSender(cfg *Config, log *logrus.Entry) (gs *Sender, err error) {
	gs = &Sender{
		log:      log,
		cluster:  cfg.Cluster,
		fields:   cfg.Fields,
		endpoint: cfg.Endpoint,
	}
	return
}
