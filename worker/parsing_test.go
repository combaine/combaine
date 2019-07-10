package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	aggConf           = "aggCore"
	moreConf          = "http_ok"
	expectedResultLen = 4 // below defined 4 test data
)

func TestParsing(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)

	Register("dummy", NewDummyFetcher)
	t.Log("dummy fetcher registered")

	pcfg, err := repository.GetParsingConfig(aggConf)
	assert.NoError(t, err, "unable to read parsingCfg %s: %s", aggConf, err)
	var parsingConfig repository.ParsingConfig
	assert.NoError(t, pcfg.Decode(&parsingConfig))

	acfg, err := repository.GetAggregationConfig(aggConf)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", aggConf, err)
	var aggregationConfig1 repository.AggregationConfig
	assert.NoError(t, acfg.Decode(&aggregationConfig1))

	acfg, err = repository.GetAggregationConfig(moreConf)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", moreConf, err)
	var aggregationConfig2 repository.AggregationConfig
	assert.NoError(t, acfg.Decode(&aggregationConfig2))

	encParsingConfig, _ := utils.Pack(parsingConfig)
	encAggregationConfigs, _ := utils.Pack(map[string]repository.AggregationConfig{
		aggConf:  aggregationConfig1,
		moreConf: aggregationConfig2,
	})

	parsingTask := ParsingTask{
		Id:                        "testId",
		Frame:                     &TimeFrame{Current: 61, Previous: 1},
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

	t.Log("start parsing")
	res, err := DoParsing(context.Background(), &parsingTask)
	t.Log("parsing completed")
	assert.NoError(t, err)
	assert.Equal(t, expectedResultLen, len(res.Data))
	for _, v := range res.Data {
		var i map[string]interface{}
		assert.NoError(t, utils.Unpack(v, &i))
		assert.Equal(t, parsingTask.Frame.Current, i["CurrTime"].(int64))
		assert.Equal(t, parsingTask.Id, i["Id"].(string))
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
