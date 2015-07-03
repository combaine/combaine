package razladki

import (
	"encoding/json"
	"testing"

	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {
	testConfig := RazladkiConfig{
		Items: map[string]string{
			"20x":      "testvalue",
			"20x.MAP1": "testvalue",
			"20x.MP2":  "testvalue",
			"20x.MP3":  "testvalue",
			"30x":      "testvalue"},
		Project: "CombaineTest",
		Host:    "127.0.0.1",
	}
	s := RazladkiSender{RazladkiConfig: &testConfig}
	s.id = "UNIQID"

	data := tasks.DataType{
		"20x": {
			"host1": 2000,
			"host3": map[string]interface{}{
				"MAP1": []interface{}{201, 301, 401},
				"MAP2": []interface{}{202, 302, 402},
			},
			"host4": map[string]interface{}{
				"MP1": 1000,
				"MP2": 1002,
			},
		},
		"30x": {
			"host2": []int{20, 30, 40},
			"host4": map[string]interface{}{
				"MP1": 1000,
				"MP2": 1002,
			},
		},
	}

	expected := &RazladkiResult{
		Timestamp: 123,
		Params: map[string]Param{
			"host1_20x": Param{
				Value: "2000",
				Meta: Meta{
					Title: "testvalue",
				},
			},
			"host4_MP2": Param{
				Value: "1002",
				Meta: Meta{
					Title: "testvalue",
				},
			},
		},
		Alarms: map[string]Alarm{
			"host1_20x": Alarm{
				Meta: Meta{
					Title: "testvalue",
				},
			},
			"host4_MP2": Alarm{
				Meta: Meta{
					Title: "testvalue",
				},
			},
		},
	}

	actual, err := s.send(data, 123)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected.Timestamp, actual.Timestamp)
	assert.Equal(t, expected.Params, actual.Params)
	assert.Equal(t, expected.Alarms, actual.Alarms)
	assert.Equal(t, expected.Timestamp, actual.Timestamp)

	mbody, _ := json.Marshal(actual)
	t.Logf("%s", mbody)

	s.Send(data, 123)
}
