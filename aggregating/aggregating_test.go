package aggregating

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/stretchr/testify/assert"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/rpc"
	"github.com/combaine/combaine/tests"
)

const (
	cfgName  = "aggCore"
	repoPath = "../tests/fixtures/configs"
)

func NewService(n string, a ...interface{}) (servicecacher.Service, error) {
	return tests.NewService(n, a...)
}

var cacher = servicecacher.NewCacher(NewService)

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

		for r := range tests.Spy {
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
					close(tests.Spy)
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
