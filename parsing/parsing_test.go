package parsing

import (
	"fmt"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/tests"
	"github.com/stretchr/testify/assert"
)

const (
	aggConf           = "aggCore"
	moreConf          = "http_ok"
	repoPath          = "../tests/fixtures/configs"
	expectedResultLen = 4 // below defined 4 test data
)

var cacher = tests.NewCacher()
var fch = make(chan string)

func NewDummy(cfg map[string]interface{}) (Fetcher, error) {
	return &fether{c: cfg}, nil
}

type fether struct {
	c configs.PluginConfig
}

func (f *fether) Fetch(task *tasks.FetcherTask) ([]byte, error) {
	fch <- f.c["timetail_url"].(string)
	return common.Pack(*task)
}

func TestParsing(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)

	Register("dummy", NewDummy)

	repo, err := configs.NewFilesystemRepository(repoPath)
	assert.NoError(t, err, "Unable to create repo %s", err)
	pcfg, err := repo.GetParsingConfig(aggConf)
	assert.NoError(t, err, "unable to read parsingCfg %s: %s", aggConf, err)
	var parsingConfig configs.ParsingConfig
	assert.NoError(t, pcfg.Decode(&parsingConfig))

	acfg, err := repo.GetAggregationConfig(aggConf)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", aggConf, err)
	var aggregationConfig1 configs.AggregationConfig
	assert.NoError(t, acfg.Decode(&aggregationConfig1))

	acfg, err = repo.GetAggregationConfig(moreConf)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", moreConf, err)
	var aggregationConfig2 configs.AggregationConfig
	assert.NoError(t, acfg.Decode(&aggregationConfig2))

	parsingTask := tasks.ParsingTask{
		CommonTask:        tasks.CommonTask{Id: "testId", PrevTime: 1, CurrTime: 61},
		Host:              "test-host",
		ParsingConfigName: aggConf,
		ParsingConfig:     parsingConfig,
		AggregationConfigs: map[string]configs.AggregationConfig{
			aggConf:  aggregationConfig1,
			moreConf: aggregationConfig2,
		},
	}
	done := make(chan struct{})
	urls := make(map[string]int)

	expectParsingResult := map[string]bool{
		"test-host.custom.Multimetrics":      false,
		"test-host.plugin.value":             false,
		"test-host.custom.FrontAggregator":   false,
		"test-host.custom.GeneralAggregator": false,
	}

	go func() {
		defer func() { done <- struct{}{} }()

		remain := expectedResultLen

		for {
			if url, ok := <-fch; ok {
				urls[url]++
			}
			if remain == 4 { // only first time
				close(fch)
			}

			k, ok := <-tests.Results
			if !ok {
				break
			}
			remain--

			var r map[string]interface{}
			assert.NoError(t, common.Unpack(k[1].([]byte), &r))
			var payload tasks.FetcherTask
			assert.NoError(t, common.Unpack(r["token"].([]byte), &payload))

			key := payload.Target
			cfg := r["config"].(map[interface{}]interface{})

			if tp, ok := cfg["type"]; ok {
				key += "." + string(tp.([]byte))
			}
			if cl, ok := cfg["class"]; ok {
				key += "." + string(cl.([]byte))
			}
			if so, ok := cfg["someOpts"]; ok {
				key += "." + string(so.([]byte))
			}

			_, ok = expectParsingResult[key]
			assert.True(t, ok, "Unexpected parsing result %s", key)
			expectParsingResult[key] = true
			if remain == 0 {
				close(tests.Results)
			}
		}
	}()

	res, err := Parsing(&parsingTask, cacher)
	assert.NoError(t, err)
	assert.Equal(t, expectedResultLen, len(res))
	for _, v := range res {
		var i map[string]interface{}
		assert.NoError(t, common.Unpack(v.([]byte), &i))
		assert.Equal(t, parsingTask.CurrTime, i["currtime"].(int64))
		assert.Equal(t, parsingTask.Id, string(i["id"].([]byte)))
	}
	select {
	case <-done:

		for parsing, test := range expectParsingResult {
			assert.True(t, test, fmt.Sprintf("parsing for %s failed", parsing))
		}
		assert.Equal(t, len(urls), 1) // only one url will be used
		// all parsings processed by one url
		assert.Equal(t, urls[parsingTask.ParsingConfig.DataFetcher["timetail_url"].(string)], 1)
		t.Log("Test done")

	case <-time.After(5 * time.Second):
		close(tests.Results)
		t.Error("Test timeout")
	}
}
