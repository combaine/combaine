package graphite

import (
	"bytes"
	"testing"

	"github.com/noxiouz/Combaine/common/tasks"
)

func TestGraphiteSend(t *testing.T) {
	grCfg := graphiteClient{
		id:      "TESTID",
		cluster: "TESTCOMBAINE",
		fields:  []string{"A", "B", "C"},
	}

	data := tasks.DataType{
		"20x": {
			"simple": 2000,
			"array":  []int{20, 30, 40},
			"map_of_array": map[string][]int{
				"MAP1": []int{201, 301, 401},
				"MAP2": []int{202, 302, 402},
			},
			"map_of_simple": map[string]int{
				"MP1": 1000,
				"MP2": 1002,
			},
		}}
	buff := new(bytes.Buffer)
	t.Log(grCfg)
	err := grCfg.sendInternal(&data, buff)
	t.Log(err)
	t.Logf("%s", buff)
}
