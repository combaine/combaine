package aggregating

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/stretchr/testify/assert"

	"github.com/Combaine/Combaine/common/configs"
	"github.com/Combaine/Combaine/common/servicecacher"
	"github.com/Combaine/Combaine/common/tasks"
	"github.com/Sirupsen/logrus"
)

const (
	cfgName  = "forAggCore"
	repoPath = "../tests/fixtures/configs"
)

func sniff(name string, args ...interface{}) {
	solution <- args
}

var mind chan []byte = make(chan []byte)
var solution chan []interface{} = make(chan []interface{})

type dummyResult struct{}

func (dr dummyResult) Extract(i interface{}) error {
	i, ok := <-mind
	if !ok {
		i = nil
		return fmt.Errorf("Ran out of knowable")
	}
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
		ch <- dummyResult{}
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
	logrus.SetLevel(logrus.DebugLevel)

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

	aggTask := tasks.AggregationTask{
		CommonTask:        tasks.CommonTask{Id: "testId", PrevTime: 1, CurrTime: 61},
		Config:            cfgName,
		ParsingConfigName: cfgName,
		ParsingConfig:     parsingConfig,
		AggregationConfig: aggregationConfig,
		Hosts: map[string][]string{
			"DC1": []string{"Host1", "Host2"},
			"DC2": []string{"Host3", "Host4"},
		},
	}

	cacher := NewCacher()
	close(mind)
	go func() {
		for r := range solution {
			var method string = string(r[0].(string))
			var payload []byte = r[1].([]byte)
			fmt.Printf("%s: %v\n", method, payload)
		}
	}()
	assert.NoError(t, Aggregating(&aggTask, cacher))
}
