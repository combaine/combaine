package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

const cfgName = "aggCore"

var (
	pcfg              repository.EncodedConfig
	acfg              repository.EncodedConfig
	parsingConfig     repository.ParsingConfig
	aggregationConfig repository.AggregationConfig
)

func TestInit(t *testing.T) {
	var err error

	pcfg, err = repository.GetParsingConfig(cfgName)
	assert.NoError(t, err, "unable to read parsingCfg %s: %s", cfgName, err)
	assert.NoError(t, pcfg.Decode(&parsingConfig))

	acfg, err = repository.GetAggregationConfig(cfgName)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", cfgName, err)
	assert.NoError(t, acfg.Decode(&aggregationConfig))
}

func TestAggregating(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	hostsPerDc := map[string][]string{
		"DC1": {"Host1", "Host2"},
		"DC2": {"Host3", "Host4"},
	}

	encHosts, _ := utils.Pack(hostsPerDc)
	encParsingConfig, _ := utils.Pack(parsingConfig)
	encAggregationConfig, _ := utils.Pack(aggregationConfig)

	aggTask := AggregatingTask{
		Id:                       "testId",
		Frame:                    &TimeFrame{Current: 61, Previous: 1},
		Config:                   cfgName,
		ParsingConfigName:        cfgName,
		EncodedParsingConfig:     encParsingConfig,
		EncodedAggregationConfig: encAggregationConfig,
		EncodedHosts:             encHosts,
		ParsingResult: &ParsingResult{
			Data: map[string][]byte{
				"Host1;appsName": []byte("Host1;appsName"),
				"Host2;appsName": []byte("Host2;appsName"),
				"Host3;appsName": []byte("Host3;appsName"),
				"Host4;appsName": []byte("Host4;appsName"),
			},
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
		sendersCount := map[string]int{
			"Host1":          0,
			"Host2":          0,
			"Host3":          0,
			"Host4":          0,
			"DC1":            0,
			"DC2":            0,
			"test-combainer": 0, // metahost
		}

		//shouldSendToSenders := len(aggregationConfig.Senders)

		t.Log("Test aggregators")
		for k, v := range expectAggregatingGroup {
			assert.True(t, v, fmt.Sprintf("aggregating for %s failed", k))
		}
		t.Log("Test senders")
		for k, v := range sendersCount {
			assert.Equal(t, len(aggregationConfig.Senders), v, fmt.Sprintf("senders for '%s' failed", k))
		}
	}()
	assert.NoError(t, DoAggregating(context.TODO(), &aggTask))
}

func TestEnqueue(t *testing.T) {
}
