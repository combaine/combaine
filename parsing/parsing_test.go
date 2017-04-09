package parsing

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/rpc"
	"github.com/combaine/combaine/tests"
	"github.com/stretchr/testify/assert"
)

const (
	aggConf           = "aggCore"
	moreConf          = "http_ok"
	repoPath          = "../tests/testdata/configs"
	expectedResultLen = 4 // below defined 4 test data
)

var fch = make(chan string, 2) // do not block fetcher

func NewDummyFetcher(cfg map[string]interface{}) (Fetcher, error) {
	return &fether{c: cfg}, nil
}

type fether struct {
	c configs.PluginConfig
}

func (f *fether) Fetch(task *common.FetcherTask) ([]byte, error) {
	fch <- string(f.c["timetail_url"].([]byte))
	return common.Pack(*task)
}

func TestParsing(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)

	Register("dummy", NewDummyFetcher)
	t.Log("dummy fetcher registered")

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

	encParsingConfig, _ := common.Pack(parsingConfig)
	encAggregationConfigs, _ := common.Pack(map[string]configs.AggregationConfig{
		aggConf:  aggregationConfig1,
		moreConf: aggregationConfig2,
	})

	parsingTask := rpc.ParsingTask{
		Id:                        "testId",
		Frame:                     &rpc.TimeFrame{Current: 61, Previous: 1},
		Host:                      "test-host",
		ParsingConfigName:         aggConf,
		EncodedParsingConfig:      encParsingConfig,
		EncodedAggregationConfigs: encAggregationConfigs,
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
		defer func() {
			close(done)
			close(fch)
			close(tests.Spy)
		}()

		for remain := expectedResultLen; remain != 0; remain-- {
			t.Logf("iteration %d", expectedResultLen-remain+1)
			select {
			case url, ok := <-fch:
				if ok {
					urls[url]++
				}
			default:
			}
			k, ok := <-tests.Spy
			if !ok {
				return
			}

			var r map[string]interface{}
			assert.NoError(t, common.Unpack(k[1].([]byte), &r))
			var payload common.FetcherTask
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
		}
	}()

	t.Log("start parsing")
	cacher := cache.NewServiceCacher(
		func(n string, a ...interface{}) (cache.Service, error) {
			return tests.NewService(n, a...)
		})
	res, err := Do(context.Background(), &parsingTask, cacher)
	t.Log("parsing completed")
	assert.NoError(t, err)
	assert.Equal(t, expectedResultLen, len(res.Data))
	for _, v := range res.Data {
		var i map[string]interface{}
		assert.NoError(t, common.Unpack(v, &i))
		assert.Equal(t, parsingTask.Frame.Current, i["currtime"].(int64))
		assert.Equal(t, parsingTask.Id, string(i["id"].([]byte)))
	}

	<-done // wait parsing complete

	for parsing, test := range expectParsingResult {
		assert.True(t, test, fmt.Sprintf("parsing for %s failed", parsing))
	}
	assert.Equal(t, len(urls), 1) // only one url will be used
	// all parsings processed by one url
	assert.Equal(t, urls[parsingConfig.DataFetcher["timetail_url"].(string)], 1)
	t.Log("Test done")
}
