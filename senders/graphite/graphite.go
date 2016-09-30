package graphite

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
)

const (
	ONE_POINT_FORMAT   = "%s.combaine.%s.%s %s %d\n"
	CONNECTION_TIMEOUT = 300 //msec
	RECONNECT_INTERVAL = 100 //msec
)

// var for testing purposes
var connectionEndpoint = ":42000"

type GraphiteSender interface {
	Send(tasks.DataType, uint64) error
}

type graphiteClient struct {
	id      string
	cluster string
	fields  []string
}

type GraphiteCfg struct {
	Cluster string   `codec:"cluster"`
	Fields  []string `codec:"Fields"`
}

type pointFormat func(common.NameStack, interface{}, uint64) string

func makePoint(format, cluster, subgroup string) pointFormat {
	return func(metric common.NameStack, value interface{}, timestamp uint64) string {
		return fmt.Sprintf(
			format,
			cluster,
			formatSubgroup(subgroup),
			strings.Join(metric, "."),
			common.InterfaceToString(value),
			timestamp,
		)
	}
}

func formatSubgroup(input string) string {
	return strings.Replace(strings.Replace(input, ".", "_", -1), "-", "_", -1)
}

func (g *graphiteClient) send(output io.Writer, data string) error {
	logger.Debugf("%s Send %s", g.id, data)
	if _, err := fmt.Fprint(output, data); err != nil {
		logger.Errf("%s Sending error: %s", g.id, err)
		connPool.Evict(output)
		return err
	}
	return nil
}

func (g *graphiteClient) sendInterface(output io.Writer, metricName common.NameStack,
	f pointFormat, value interface{}, timestamp uint64) error {
	data := f(metricName, value, timestamp)
	return g.send(output, data)
}

func (g *graphiteClient) sendSlice(output io.Writer, metricName common.NameStack, f pointFormat,
	rv reflect.Value, timestamp uint64) error {

	if len(g.fields) == 0 || len(g.fields) != rv.Len() {
		return fmt.Errorf("%s Unable to send a slice. Fields len %d, len of value %d",
			g.id, len(g.fields), rv.Len())
	}

	for i := 0; i < rv.Len(); i++ {
		metricName.Push(g.fields[i])

		item := rv.Index(i).Interface()
		err := g.sendInterface(output, metricName, f, common.InterfaceToString(item), timestamp)
		if err != nil {
			return err
		}

		metricName.Pop()
	}
	return nil
}

func (g *graphiteClient) sendMap(output io.Writer, metricName common.NameStack, f pointFormat,
	rv reflect.Value, timestamp uint64) (err error) {

	keys := rv.MapKeys()
	for _, key := range keys {
		// Push key of map
		metricName.Push(common.InterfaceToString(key.Interface()))

		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		logger.Debugf("%s Item of key %s is: %v", g.id, key, itemInterface.Kind())

		switch itemInterface.Kind() {
		case reflect.Slice, reflect.Array:
			err = g.sendSlice(output, metricName, f, itemInterface, timestamp)
		case reflect.Map:
			err = g.sendMap(output, metricName, f, itemInterface, timestamp)
		default:
			err = g.sendInterface(output, metricName, f,
				common.InterfaceToString(itemInterface.Interface()), timestamp)
		}
		if err != nil {
			break
		}
		// Pop key of map
		metricName.Pop()
	}
	return
}

func (g *graphiteClient) sendInternal(data *tasks.DataType, timestamp uint64, output io.Writer) (err error) {
	metricName := make(common.NameStack, 0, 3)

	for aggname, subgroupsAndValues := range *data {
		logger.Infof("%s Handle aggregate named %s", g.id, aggname)

		metricName.Push(aggname)
		for subgroup, value := range subgroupsAndValues {
			pointFormatter := makePoint(ONE_POINT_FORMAT, g.cluster, subgroup)
			rv := reflect.ValueOf(value)
			logger.Debugf("%s %s", g.id, rv.Kind())

			switch rv.Kind() {
			case reflect.Slice, reflect.Array:
				err = g.sendSlice(output, metricName, pointFormatter, rv, timestamp)
			case reflect.Map:
				err = g.sendMap(output, metricName, pointFormatter, rv, timestamp)
			default:
				err = g.sendInterface(output, metricName, pointFormatter, common.InterfaceToString(value), timestamp)
			}
			if err != nil {
				break
			}
		}
		metricName.Pop()
	}
	return
}

func (g *graphiteClient) Send(data tasks.DataType, timestamp uint64) error {
	if len(data) == 0 {
		return fmt.Errorf("%s Empty data. Nothing to send.", g.id)
	}

	sock, err := connPool.Get(connectionEndpoint, 3, CONNECTION_TIMEOUT)
	if err != nil {
		return err
	}

	return g.sendInternal(&data, timestamp, sock)
}

func NewGraphiteClient(cfg *GraphiteCfg, id string) (gs GraphiteSender, err error) {
	gs = &graphiteClient{
		id:      id,
		cluster: cfg.Cluster,
		fields:  cfg.Fields,
	}
	return
}
