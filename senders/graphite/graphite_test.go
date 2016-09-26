package graphite

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/combaine/combaine/common/tasks"
)

func TestGraphiteSend(t *testing.T) {
	grCfg := graphiteClient{
		id:      "TESTID",
		cluster: "TESTCOMBAINE",
		fields:  []string{"A", "B", "C"},
	}

	tms := fmt.Sprintf("%d", 100)
	var (
		expected = map[string]struct{}{
			"TESTCOMBAINE.combaine.simple.20x 2000 " + tms:                   {},
			"TESTCOMBAINE.combaine.array.20x.A 20 " + tms:                    {},
			"TESTCOMBAINE.combaine.array.20x.B 30 " + tms:                    {},
			"TESTCOMBAINE.combaine.array.20x.C 40 " + tms:                    {},
			"TESTCOMBAINE.combaine.map_of_simple.20x.MP1 1000 " + tms:        {},
			"TESTCOMBAINE.combaine.map_of_simple.20x.MP2 1002 " + tms:        {},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP1.A 201 " + tms:       {},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP1.B 301 " + tms:       {},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP1.C 401 " + tms:       {},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP2.A 202 " + tms:       {},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP2.B 302 " + tms:       {},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP2.C 402 " + tms:       {},
			"TESTCOMBAINE.combaine.simple.30x 2000 " + tms:                   {},
			"TESTCOMBAINE.combaine.map_of_map.20x.MAPMAP1.MPMP1 1000 " + tms: {},
			"TESTCOMBAINE.combaine.map_of_map.20x.MAPMAP1.MPMP2 1002 " + tms: {},
		}
	)

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
			"map_of_map": map[string]interface{}{
				"MAPMAP1": map[string]interface{}{
					"MPMP1": 1000,
					"MPMP2": 1002,
				},
			},
		},
		"30x": {
			"simple": 2000,
		},
	}
	buff := new(bytes.Buffer)
	err := grCfg.sendInternal(&data, 100, buff)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range strings.Split(buff.String(), "\n") {
		if len(item) == 0 {
			continue
		}
		_, ok := expected[item]
		if !ok {
			t.Logf("%s is not in the expected\n", item)
			t.Fail()
		}

	}

}
