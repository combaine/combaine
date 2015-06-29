package cbb

import (
	//"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {
	data := tasks.DataType{
		"cbb": {
			"music-stable-xfront-fol": map[string]interface{}{
				"2xx": map[string]interface{}{
					"85.172.10.226": 74.90206796028059},
			},
			"music-stable-xfront": map[string]interface{}{
				"4xx": map[string]interface{}{
					"112.100.21.30": 74.75854383358097,
					"44.32.218.444": 73.74793615850301,
				},
				"5xx": map[string]interface{}{
					"178.34.104.77": 70.77070119037275,
					"194.9.15.19":   72.78298485940877,
				},
				"2xx": map[string]interface{}{
					"193.111.140.57": 72.52881101845779},
			},
			"music-stable-xfront-sas": map[string]interface{}{
				"2xx": 0,
				"4xx": []string{"44.32.218.444", "55.32.218.555"},
			},
			"music-stable-xfront-ugr": map[string]interface{}{},
			"music-stable-xfront-iva": map[string]interface{}{},
			"music-stable-xfront-myt": map[string]interface{}{},
		},
	}
	testQ := func(cfg *CBBConfig, data tasks.DataType) []url.URL {
		s, err := NewCBBClient(cfg, "testCbbClient")
		if err != nil {
			t.Logf("Unexpected error %s", err)
			t.Fail()
		}
		res, err := s.send(data, uint64(time.Now().Unix()))
		if err != nil {
			t.Logf("%v", err)
			t.Fail()
		}
		requests := make([]url.URL, 0)
		for u := range res {
			requests = append(requests, u)
		}
		return requests
	}

	testConfig := CBBConfig{
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
	dataOneIp := tasks.DataType{
		"cbb": {
			"music-stable-xfront": map[string]interface{}{
				"5xx": map[string]interface{}{
					"9.9.9.9": 74.90206796028059},
			},
		},
	}
	requests = testQ(&testConfig, dataOneIp)
	assert.Equal(t, len(requests), 1)

	// tabletype != 2
	testConfig.TableType = 0
	query := requests[0].RawQuery
	assert.True(t, strings.Contains(query, "description=Text"))
	assert.True(t, strings.Contains(query, "range_src=9.9.9.9") &&
		strings.Contains(query, "range_dst=9.9.9.9"))
	assert.Equal(t, requests[0].Path, "path")

	// tabletype == 2
	testConfig.TableType = 2
	requests = testQ(&testConfig, dataOneIp)
	query = requests[0].RawQuery
	assert.True(t, strings.Contains(query, "description=Text"))
	assert.True(t, strings.Contains(query, "net_ip=9.9.9.9") &&
		strings.Contains(query, "net_mask=32"))
	assert.Equal(t, requests[0].Path, "path")

	t.Logf("%v", query)
}
