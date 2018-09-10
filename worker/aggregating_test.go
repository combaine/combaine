package worker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/rpc"
	tests "github.com/combaine/combaine/testdata"
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

	testDone := make(chan struct{})
	hostsPerDc := map[string][]string{
		"DC1": {"Host1", "Host2"},
		"DC2": {"Host3", "Host4"},
	}

	encHosts, _ := common.Pack(hostsPerDc)
	encParsingConfig, _ := common.Pack(parsingConfig)
	encAggregationConfig, _ := common.Pack(aggregationConfig)

	aggTask := rpc.AggregatingTask{
		Id:                       "testId",
		Frame:                    &rpc.TimeFrame{Current: 61, Previous: 1},
		Config:                   cfgName,
		ParsingConfigName:        cfgName,
		EncodedParsingConfig:     encParsingConfig,
		EncodedAggregationConfig: encAggregationConfig,
		EncodedHosts:             encHosts,
		ParsingResult: &rpc.ParsingResult{
			Data: map[string][]byte{
				"Host1;appsName": []byte("Host1;appsName"),
				"Host2;appsName": []byte("Host2;appsName"),
				"Host3;appsName": []byte("Host3;appsName"),
				"Host4;appsName": []byte("Host4;appsName"),
			},
		},
	}

	go func() {
		defer close(testDone)

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

		shouldSendToSenders := len(aggregationConfig.Senders)

		logrus.Warn("Enter Spy loop")
		for r := range tests.Spy { // r == []interface{cache.Service, []byte}
			method := string(r[0].(string))
			if method == "stop" {
				break
			}
			switch method {
			case "aggregate_group":
				var akeys []string

				var payload common.AggregateGropuPayload
				assert.NoError(t, common.Unpack(r[1].([]byte), &payload))

				for _, v := range payload.Data {
					akeys = append(akeys, string(v[:5])) // text for expectAggregatingGroup
				}
				sort.Strings(akeys)
				_k := strings.Join(akeys, "")
				_, ok := expectAggregatingGroup[_k]
				assert.True(t, ok, "Unexpected aggregate %s", _k)
				expectAggregatingGroup[_k] = true

			case "send":
				shouldSendToSenders--
				logrus.Warnf("Send: shouldSendToSenders=%v", shouldSendToSenders)

				var payload common.SenderPayload
				assert.NoError(t, common.Unpack(r[1].([]byte), &payload))
				for _, v := range payload.Data {
					_, ok := sendersCount[v.Tags["name"]]
					assert.True(t, ok, "Unexpected senders payload %s", v.Tags["name"])
					sendersCount[v.Tags["name"]]++
				}
				if shouldSendToSenders == 0 {
					tests.Spy <- []interface{}{"stop", ""}
				}
			}
		}
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
	<-testDone
}

func TestEnqueue(t *testing.T) {
	go func() {
		for r := range tests.Spy {
			method := string(r[0].(string))
			if method == "stop" {
				break
			}
		}
	}()

	tests.Rake <- fmt.Errorf("test")

	app, _ := cacher.Get("respErr")
	resp, err := enqueue("respErr", app, &[]byte{})
	assert.Nil(t, resp)
	assert.Error(t, err)

	specialData := []byte("returnError")
	resp, _ = enqueue("respErr", app, &specialData)
	assert.Nil(t, resp)
	assert.Error(t, err)

	// just coverage hit
	// TODO(sakateka): make real testing
	tests.Rake <- fmt.Errorf("test")
	hostsPerDc := map[string][]string{"DC1": {"H1", "H2"}, "DC2": {"H3", "H4"}}
	encHosts, _ := common.Pack(hostsPerDc)
	encParsingConfig, _ := common.Pack(parsingConfig)

	acfg, err = repository.GetAggregationConfig("bad" + cfgName)
	assert.NoError(t, acfg.Decode(&aggregationConfig))
	encAggregationConfig, _ := common.Pack(aggregationConfig)

	aggTask := rpc.AggregatingTask{
		Id:                       "testId",
		Frame:                    &rpc.TimeFrame{Current: 61, Previous: 1},
		Config:                   cfgName,
		ParsingConfigName:        cfgName,
		EncodedParsingConfig:     encParsingConfig,
		EncodedAggregationConfig: encAggregationConfig,
		EncodedHosts:             encHosts,
		ParsingResult: &rpc.ParsingResult{
			Data: map[string][]byte{"Host1;appsName": []byte("Host1;appsName")},
		}}
	assert.NoError(t, DoAggregating(context.TODO(), &aggTask))

	acfg, err = repository.GetAggregationConfig("notPerHost" + cfgName)
	assert.NoError(t, acfg.Decode(&aggregationConfig))

	encAggregationConfig, _ = common.Pack(aggregationConfig)
	aggTask.EncodedAggregationConfig = encAggregationConfig
	assert.NoError(t, DoAggregating(context.TODO(), &aggTask))

	aggTask.ParsingResult = &rpc.ParsingResult{
		Data: map[string][]byte{"H1;appsName": []byte("H1;appsName")},
	}
	assert.NoError(t, DoAggregating(context.TODO(), &aggTask))

	tests.Spy <- []interface{}{"stop", ""}
}
