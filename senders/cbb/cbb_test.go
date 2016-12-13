package cbb

import (
	"testing"
	"time"

	"github.com/combaine/combaine/common/tasks"
	"github.com/stretchr/testify/assert"
)

func TestSend(t *testing.T) {
	data := []tasks.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "hosts-group-1",
			"metahost": "meta.host.name", "aggregate": "cbb"},
			Result: map[string]interface{}{
				"2xx": map[string]interface{}{
					"10.12.10.226": 74.90206796028059},
			}},
		{Tags: map[string]string{"type": "host", "name": "hosts-group-2",
			"metahost": "meta.host.name", "aggregate": "cbb"},
			Result: map[string]interface{}{
				"4xx": map[string]interface{}{
					"10.100.21.30":  74.75854383358097,
					"10.32.218.444": 73.74793615850301,
				},
				"5xx": map[string]interface{}{
					"10.34.104.77": 70.77070119037275,
					"10.9.15.19":   72.78298485940877,
				},
				"2xx": map[string]interface{}{
					"10.111.140.57": 72.52881101845779},
			}},
		{Tags: map[string]string{"type": "host", "name": "host1",
			"metahost": "host1", "aggregate": "cbb"},
			Result: map[string]interface{}{
				"2xx": 0,
				"4xx": []string{"10.32.218.444", "10.32.218.555"},
			}},
		{Tags: map[string]string{"type": "host", "name": "host2",
			"metahost": "host2", "aggregate": "cbb"},
			Result: map[string]interface{}{}},
		{Tags: map[string]string{"type": "host", "name": "host3",
			"metahost": "host3", "aggregate": "cbb"},
			Result: map[string]interface{}{}},
		{Tags: map[string]string{"type": "host", "name": "host4",
			"metahost": "host4", "aggregate": "cbb"},
			Result: map[string]interface{}{}},
	}

	testQ := func(cfg *Config, data []tasks.AggregationResult) []string {
		s, err := NewCBBClient(cfg, "testCbbClient")
		if err != nil {
			t.Logf("Unexpected error %s", err)
			t.Fail()
		}
		res, err := s.send(data, uint64(time.Now().Unix()))
		assert.NoError(t, err)
		return res
	}

	testConfig := Config{
		Items:      []string{"5bad", "cbb.2xx", "cbb.5xx"},
		Flag:       112,
		Host:       "localhost",
		TableType:  1,
		ExpireTime: 3600,
	}

	// 4 ip
	requests := testQ(&testConfig, data)
	assert.Equal(t, len(requests), 4)

	// 6
	testConfig.Items = []string{"cbb.2xx", "cbb.4xx", "cbb.5xx"}
	requests = testQ(&testConfig, data)
	assert.Equal(t, len(requests), 6)

	//1
	testConfig.Items = []string{"cbb.5xx"}
	testConfig.Description = "Text"
	testConfig.Path = "path"
	dataOneIP := []tasks.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "hosts-group-1",
			"metahost": "meta.host.name", "aggregate": "cbb"},
			Result: map[string]interface{}{
				"5xx": map[string]interface{}{
					"10.9.9.9": 74.90206796028059},
			}},
	}
	requests = testQ(&testConfig, dataOneIP)
	assert.Equal(t, len(requests), 1)

	// tabletype != 2
	testConfig.TableType = 0
	query := requests[0]
	assert.Contains(t, query, "description=Text")
	assert.Contains(t, query, "range_src=10.9.9.9")
	assert.Contains(t, query, "range_dst=10.9.9.9")
	assert.Contains(t, query, "path")

	// tabletype == 2
	testConfig.TableType = 2
	requests = testQ(&testConfig, dataOneIP)
	query = requests[0]
	assert.Contains(t, query, "description=Text")
	assert.Contains(t, query, "net_ip=10.9.9.9")
	assert.Contains(t, query, "net_mask=32")
	assert.Contains(t, query, "path")

	t.Logf("%v", query)
}
