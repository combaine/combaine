package agave

import (
	"testing"

	"github.com/Combaine/Combaine/common/tasks"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {
	testConfig := AgaveConfig{
		Id:            "testID",
		Items:         []string{"20x", "20x.MAP1", "20x.MP2", "20x.MP3", "30x"},
		Hosts:         []string{"blabla:8080"},
		GraphName:     "GraphName",
		GraphTemplate: "graph_template",
		Fields:        []string{"A", "B", "C"},
		Step:          300,
	}
	s := AgaveSender{testConfig}

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

	expected := map[string][]string{
		"host1": {"20x:2000"},
		"host3": {"A:201+B:301+C:401"},
		"host4": {"MP2:1002"},
		"host2": {"A:20+B:30+C:40"},
	}

	actual, err := s.send(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)
}
