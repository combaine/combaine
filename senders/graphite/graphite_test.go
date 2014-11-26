package graphite

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/noxiouz/Combaine/common/tasks"
)

func TestGraphiteSend(t *testing.T) {
	grCfg := graphiteClient{
		id:      "TESTID",
		cluster: "TESTCOMBAINE",
		fields:  []string{"A", "B", "C"},
	}

	tms := fmt.Sprintf("%d", time.Now().Unix())
	var (
		expected = map[string]struct{}{
			"TESTCOMBAINE.combaine.simple.20x 2000 " + tms:             struct{}{},
			"TESTCOMBAINE.combaine.array.20x.A 20 " + tms:              struct{}{},
			"TESTCOMBAINE.combaine.array.20x.B 30 " + tms:              struct{}{},
			"TESTCOMBAINE.combaine.array.20x.C 40 " + tms:              struct{}{},
			"TESTCOMBAINE.combaine.map_of_simple.20x.MP1 1000 " + tms:  struct{}{},
			"TESTCOMBAINE.combaine.map_of_simple.20x.MP2 1002 " + tms:  struct{}{},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP1.A 201 " + tms: struct{}{},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP1.B 301 " + tms: struct{}{},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP1.C 401 " + tms: struct{}{},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP2.A 202 " + tms: struct{}{},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP2.B 302 " + tms: struct{}{},
			"TESTCOMBAINE.combaine.map_of_array.20x.MAP2.C 402 " + tms: struct{}{},
		}
	)

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
	err := grCfg.sendInternal(&data, buff)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range strings.Split(buff.String(), "\n") {
		if len(item) == 0 {
			continue
		}
		_, ok := expected[item]
		if !ok {
			t.Logf("%s is not in the expected", item)
			t.Fail()
		}

	}

}
