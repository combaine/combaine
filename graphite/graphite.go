package graphite

import (
	"fmt"
	"net"
	"reflect"
	// "text/template"
	"time"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/logger"
)

const onePointTemplateText = "{{.cluster}}.combaine.{{.metahost}}.{{.item}} {{.value}} {{.timestamp}}"

const onePointFormat = "%s.combaine.%s.%s %s %d"

// var onePointTemplate *template.Template = template.Must(template.New("URL").Parse(onePointTemplateText))

type GraphiteSender interface {
	Send(common.DataType) error
}

const connectionTimeout = 900       //msec
const connectionEndpoint = ":42000" //msec

type graphiteClient struct {
	cluster string
	fields  []string
}

type GraphiteCfg struct {
	Cluster string   `codec:"cluster"`
	Fields  []string `codec:"Fields"`
}

/*
common.DataType:
{
	"20x": {
		"group1": 2000,
		"group2": [20, 30, 40]
	},
}
*/

func (g *graphiteClient) Send(data common.DataType) (err error) {
	if len(data) == 0 {
		return fmt.Errorf("Empty data. Nothing to send.")
	}

	/*
		Create socket here.
	*/
	sock, err := net.DialTimeout("tcp", connectionEndpoint, time.Microsecond*connectionTimeout)
	if err != nil {
		logger.Errf("Unable to connect to daemon %s: %s", connectionEndpoint, err)
		return
	}
	defer sock.Close()

	for aggname, subgroupsAndValues := range data {
		logger.Debugf("Handle aggregate named %s", aggname)
		for subgroup, value := range subgroupsAndValues {

			rv := reflect.ValueOf(value)
			switch kind := rv.Kind(); kind {
			case reflect.Slice, reflect.Array:
				if len(g.fields) == 0 || len(g.fields) != rv.Len() {
					logger.Errf("Unable to send a slice. Fields len %d, len of value %d", len(g.fields), rv.Len())
					continue
				}
				for i := 0; i < rv.Len(); i++ {
					itemInterface := rv.Index(i).Interface()
					toSend := fmt.Sprintf(
						onePointFormat,
						g.cluster,
						subgroup,
						fmt.Sprintf("%s.%s", aggname, g.fields[i]),
						common.InterfaceToString(itemInterface),
						time.Now().Unix())

					logger.Infof("Send %s", toSend)
					if _, err = fmt.Fprint(sock, toSend); err != nil {
						logger.Errf("Sending error: %s", err)
						return err
					}
				}

			default:
				toSend := fmt.Sprintf(
					onePointFormat,
					g.cluster,
					subgroup,
					aggname,
					common.InterfaceToString(value),
					time.Now().Unix())

				logger.Infof("Send %s", toSend)
				if _, err = fmt.Fprint(sock, toSend); err != nil {
					logger.Errf("Sending error: %s", err)
					return err
				}
			}
		}

	}
	return
}

func NewGraphiteClient(cfg *GraphiteCfg) (gs GraphiteSender, err error) {
	gs = &graphiteClient{
		cluster: cfg.Cluster,
		fields:  cfg.Fields,
	}

	return
}
