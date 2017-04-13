package razladki

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/stretchr/testify/assert"
)

func init() {
	InitializeLogger(func() {
		logger.CocaineLog = logger.LocalLogger()
	})
}

func TestSend(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/save_new_data_json":
			fmt.Fprintln(w, "200")
		}
	}))

	defer ts.Close()
	testConfig := Config{
		Items: map[string]string{
			"40x":              "testvalue",
			"20x:MAP1.2xx":     "testvalue", // map in map not supported
			"20x:MP2":          "testvalue",
			"20x:MP3":          "testvalue",
			"some.agg.20x:MP2": "testvalue",
			"30x":              "testvalue"},
		Project: "CombaineTest",
		Host:    ts.Listener.Addr().String(),
	}
	s := Sender{Config: &testConfig}
	s.id = "UNIQID"

	data := []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "host1", "metahost": "host1", "aggregate": "40x"},
			Result: 2000},
		{Tags: map[string]string{"type": "datacenter", "name": "DC1", "metahost": "host3", "aggregate": "20x"},
			Result: map[string]interface{}{
				"MAP1": map[string]interface{}{
					"2xx": 201,
					"3xx": 301,
					"4xx": 401,
				},
				"MAP2": []interface{}{202, 302, 402},
			}},
		{Tags: map[string]string{"type": "host", "name": "host4", "metahost": "host4", "aggregate": "20x"},
			Result: map[string]interface{}{
				"MP1": 1000,
				"MP2": 1002,
			}},
		{Tags: map[string]string{"type": "host", "name": "host5", "metahost": "host5", "aggregate": "some.agg.20x"},
			Result: map[string]interface{}{
				"MP2": 1002,
			}},
		{Tags: map[string]string{"type": "metahost", "name": "host2", "metahost": "host2", "aggregate": "30x"},
			Result: []int{20, 30, 40}},
		{Tags: map[string]string{"type": "metahost", "name": "host4", "metahost": "host4", "aggregate": "30x"},
			Result: map[string]interface{}{
				"MP1": 1000,
				"MP2": 1002,
			}},
	}

	expected := &result{
		Timestamp: 123,
		Params: map[string]Param{
			"host1_40x": {Value: "2000", Meta: Meta{Title: "testvalue"}},
			"host4_MP2": {Value: "1002", Meta: Meta{Title: "testvalue"}},
			"host5_MP2": {Value: "1002", Meta: Meta{Title: "testvalue"}},
		},
		Alarms: map[string]Alarm{
			"host1_40x": {Meta: Meta{Title: "testvalue"}},
			"host4_MP2": {Meta: Meta{Title: "testvalue"}},
			"host5_MP2": {Meta: Meta{Title: "testvalue"}},
		},
	}

	actual, err := s.send(data, 123)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)

	mbody, _ := json.Marshal(actual)
	t.Logf("%s", mbody)

	err = s.Send(context.Background(), data, 123)
	assert.NoError(t, err)
}
