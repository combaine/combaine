package aggregating

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/net/context"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/stretchr/testify/assert"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/common/tasks"
)

const (
	cfgName  = "aggCore"
	repoPath = "../tests/fixtures/configs"
)

var results = make(chan []interface{})

func sniff(name string, args ...interface{}) {
	results <- args
}

type dummyResult []byte

func (dr dummyResult) Extract(i interface{}) error {
	common.Unpack(dr, i)
	return nil
}
func (dr dummyResult) Err() error {
	return nil
}

type dummyService struct{}

func (ds *dummyService) Call(name string, args ...interface{}) chan cocaine.ServiceResult {
	sniff(name, args...)
	ch := make(chan cocaine.ServiceResult)
	go func() {
		ch <- dummyResult(args[1].([]byte))
	}()
	return ch
}
func (ds *dummyService) Close() { /* pass */ }
func NewService() servicecacher.Service {
	return &dummyService{}
}

type cache map[string]servicecacher.Service

type cacher struct {
	mutex sync.Mutex
	data  atomic.Value
}

func NewCacher() servicecacher.Cacher {
	c := &cacher{}
	c.data.Store(make(cache))

	return c
}

func (c *cacher) Get(name string) (s servicecacher.Service, err error) {
	s, ok := c.get(name)
	if ok {
		return
	}

	s, err = c.create(name)
	return
}

func (c *cacher) get(name string) (s servicecacher.Service, ok bool) {
	s, ok = c.data.Load().(cache)[name]
	return
}

func (c *cacher) create(name string) (servicecacher.Service, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	s, ok := c.get(name)
	if ok {
		return s, nil
	}

	s = NewService()
	data := c.data.Load().(cache)
	data[name] = s.(servicecacher.Service)
	c.data.Store(data)

	return s, nil
}

func TestAggregating(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)

	repo, err := configs.NewFilesystemRepository(repoPath)
	assert.NoError(t, err, "Unable to create repo %s", err)
	pcfg, err := repo.GetParsingConfig(cfgName)
	assert.NoError(t, err, "unable to read parsingCfg %s: %s", cfgName, err)
	acfg, err := repo.GetAggregationConfig(cfgName)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", cfgName, err)

	var parsingConfig configs.ParsingConfig
	assert.NoError(t, pcfg.Decode(&parsingConfig))

	var aggregationConfig configs.AggregationConfig
	assert.NoError(t, acfg.Decode(&aggregationConfig))

	hostsPerDc := map[string][]string{
		"DC1": {"Host1", "Host2"},
		"DC2": {"Host3", "Host4"},
	}

	aggTask := tasks.AggregationTask{
		CommonTask:        tasks.CommonTask{Id: "testId", PrevTime: 1, CurrTime: 61},
		Config:            cfgName,
		ParsingConfigName: cfgName,
		ParsingConfig:     parsingConfig,
		AggregationConfig: aggregationConfig,
		Hosts:             hostsPerDc,
		ParsingResult: tasks.ParsingResult{
			"Host1;aggCore;aggCore;appsName;61": "Host1;aggCore;aggCore;appsName;61",
			"Host2;aggCore;aggCore;appsName;61": "Host2;aggCore;aggCore;appsName;61",
			"Host3;aggCore;aggCore;appsName;61": "Host3;aggCore;aggCore;appsName;61",
			"Host4;aggCore;aggCore;appsName;61": "Host4;aggCore;aggCore;appsName;61",
		},
	}

	go func() {
		expectAggregatingGroup := map[string]bool{
			"Host1":                false,
			"Host2":                false,
			"Host3":                false,
			"Host4":                false,
			"Host1Host2":           false,
			"Host3Host4":           false,
			"Host1Host2Host3Host4": false,
		}
		expectSenders := map[string]int{
			"Host1":              0,
			"Host2":              0,
			"Host3":              0,
			"Host4":              0,
			"test-combainer-DC1": 0,
			"test-combainer-DC2": 0,
			"test-combainer":     0, // metahost
		}

		shouldSendToSenders := 2

		for r := range results {
			method := string(r[0].(string))
			switch method {
			case "aggregate_group":
				var akeys []string

				var payload []interface{}
				assert.NoError(t, common.Unpack(r[1].([]byte), &payload))

				tmp := payload[2].([]interface{})
				for _, v := range tmp {
					akeys = append(akeys, string(v.([]byte)[:5]))
				}
				sort.Strings(akeys)
				_k := strings.Join(akeys, "")
				_, ok := expectAggregatingGroup[_k]
				assert.True(t, ok, "Unexpected aggregate %s", _k)
				expectAggregatingGroup[_k] = true

			case "send":
				shouldSendToSenders--

				var payload tasks.SenderPayload
				assert.NoError(t, common.Unpack(r[1].([]byte), &payload))
				for _, v := range payload.Data {
					for _k := range v {
						_, ok := expectSenders[_k]
						assert.True(t, ok, "Unexpected senders payload %s", _k)
						expectSenders[_k]++
					}
				}
				if shouldSendToSenders == 0 {
					close(results)
				}
			}
		}
		t.Log("Test aggregators")
		for k, v := range expectAggregatingGroup {
			assert.True(t, v, fmt.Sprintf("aggregating for %s failed", k))
		}
		t.Log("Test senders")
		for k, v := range expectSenders {
			assert.Equal(t, v, 2, fmt.Sprintf("sedners for '%s' failed", k))
		}
	}()
	cacher := NewCacher()
	assert.NoError(t, Do(context.Background(), &aggTask, cacher))
}
