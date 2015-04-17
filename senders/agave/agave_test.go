package agave

import (
	"testing"

	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {
	testConfig := AgaveConfig{
		Id:            "testID",
		Items:         []string{"30x", "20x"},
		Hosts:         []string{"blabla:8080"},
		GraphName:     "GraphName",
		GraphTemplate: "graph_template",
		Fields:        []string{"A", "B", "C"},
		Step:          300,
	}
	s := AgaveSender{testConfig}
	t.Log(s)

	data := tasks.DataType{
		"20x": {
			"simple": 2000,
			"array":  []int{20, 30, 40},
			"map_of_array": map[string]interface{}{
				"MAP1": []interface{}{201, 301, 401},
				"MAP2": []interface{}{202, 302, 402},
			},
			"map_of_simple": map[string]interface{}{
				"MP1": 1000,
				"MP2": 1002,
			},
		},
		"30x": {
			"simple": 2000,
		},
	}

	expected := map[string][]string{
		"simple":        []string{"30x:2000", "20x:2000"},
		"array":         []string{"A:20+B:30+C:40"},
		"map_of_array":  []string{"MAP1_A:201+MAP1_B:301+MAP1_C:401", "MAP2_A:202+MAP2_B:302+MAP2_C:402"},
		"map_of_simple": []string{"20x_MP1:1000", "20x_MP2:1002"},
	}

	actual, err := s.send(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)
}
