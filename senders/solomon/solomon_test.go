package solomon

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/stretchr/testify/assert"
)

func TestSolomonSend(t *testing.T) {

	var (
		ts           uint64
		expectedData map[string]solomonPush
	)

	solCfg := solomonClient{
		id:      "testId",
		project: "combaine",
		cluster: "backend",
	}

	simpleOneItemData := tasks.DataType{
		"serviceName": {
			"hostName": map[string]interface{}{
				"sensorName": 17,
			},
		},
	}

	simpleOneItemJSON := `{
		"hostName": {"commonLabels": {
			"project": "combaine",
			"cluster": "backend",
			"host":	   "hostName",
			"service": "serviceName"
		},
		"sensors": [{"labels": {"sensor": "sensorName"}, "ts": 10, "value": 17}]}
	}`

	if err := json.Unmarshal([]byte(simpleOneItemJSON), &expectedData); err != nil {
		t.Fatal(err)
	}
	ts = 10
	data, err := solCfg.sendInternal(&simpleOneItemData, ts)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range data {
		exp, err := json.Marshal(expectedData[v.CommonLabels.Host])
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		logger.Debugf("Expected json: %s", string(exp))
		if string(exp) != string(got) {
			t.Fatal(fmt.Sprintf("Unexpected result %s != %s", string(exp), string(got)))
		}
	}

	// Complex test
	solCfg = solomonClient{
		id:      "TESTID",
		project: "TEST",
		cluster: "TESTCOMBAINE",
		fields:  []string{"25_prc", "50_prc", "99_prc"},
	}
	app1Data := tasks.DataType{
		"app1": {
			"group1": map[string]interface{}{
				"ArrayOv": []int{20, 30, 40},
			},
			"group2": map[string]interface{}{
				"map_of_array": map[string]interface{}{
					"MoAv1": []interface{}{201, 301, 401},
					"MoAv2": []interface{}{202, 302, 402},
				},
			},
			"host4": map[string]interface{}{
				"map_of_simple": map[string]interface{}{
					"MoSv1": 1001,
					"MoSv2": 1002,
				},
			},
			"host5": map[string]interface{}{
				"map_of_map": map[string]interface{}{
					"MAPMAP1": map[string]interface{}{
						"MoMv1": 76,
						"MoMv2": 77,
					},
					"MAPMAP2": map[string]interface{}{
						"MoMv1": 87,
						"MoMv2": 88,
					},
				},
			},
		},
	}
	app1JSON := `{
	"group1":{"commonLabels": {
			"project": "TEST",
			"cluster": "TESTCOMBAINE",
			"host":	   "group1",
			"service": "app1"
		},
		"sensors": [
			{"labels": {"sensor": "ArrayOv.25_prc"}, "ts": 200, "value": 20},
			{"labels": {"sensor": "ArrayOv.50_prc"}, "ts": 200, "value": 30},
			{"labels": {"sensor": "ArrayOv.99_prc"}, "ts": 200, "value": 40}
		]
	},
	"group2":{"commonLabels": {
			"project": "TEST",
			"cluster": "TESTCOMBAINE",
			"host":	   "group2",
			"service": "app1"
		},
		"sensors": [
			{"labels": {"sensor": "map_of_array.MoAv1.25_prc"}, "ts": 200, "value": 201},
			{"labels": {"sensor": "map_of_array.MoAv1.50_prc"}, "ts": 200, "value": 301},
			{"labels": {"sensor": "map_of_array.MoAv1.99_prc"}, "ts": 200, "value": 401},
			{"labels": {"sensor": "map_of_array.MoAv2.25_prc"}, "ts": 200, "value": 202},
			{"labels": {"sensor": "map_of_array.MoAv2.50_prc"}, "ts": 200, "value": 302},
			{"labels": {"sensor": "map_of_array.MoAv2.99_prc"}, "ts": 200, "value": 402}
		]
	},
	"host4":{"commonLabels": {
			"project": "TEST",
			"cluster": "TESTCOMBAINE",
			"host":	   "host4",
			"service": "app1"
		},
		"sensors": [
			{"labels": {"sensor": "map_of_simple.MoSv1"}, "ts": 200, "value": 1001},
			{"labels": {"sensor": "map_of_simple.MoSv2"}, "ts": 200, "value": 1002}
		]
	},
	"host5":{"commonLabels": {
			"project": "TEST",
			"cluster": "TESTCOMBAINE",
			"host":	   "host5",
			"service": "app1"
		},
		"sensors": [
			{"labels": {"sensor": "map_of_map.MAPMAP1.MoMv1"}, "ts": 200, "value": 76},
			{"labels": {"sensor": "map_of_map.MAPMAP1.MoMv2"}, "ts": 200, "value": 77},
			{"labels": {"sensor": "map_of_map.MAPMAP2.MoMv1"}, "ts": 200, "value": 87},
			{"labels": {"sensor": "map_of_map.MAPMAP2.MoMv2"}, "ts": 200, "value": 88}
		]
	}}`

	if err := json.Unmarshal([]byte(app1JSON), &expectedData); err != nil {
		t.Fatal(err)
	}
	ts = 200
	data, err = solCfg.sendInternal(&app1Data, ts)
	if err != nil {
		t.Fatal(err)
	}
	for _, gotValue := range data {
		expValue := expectedData[gotValue.CommonLabels.Host]
		assert.Equal(t, len(expValue.Sensors), len(gotValue.Sensors))
		exp, _ := json.Marshal(expValue.CommonLabels)
		got, _ := json.Marshal(gotValue.CommonLabels)
		logger.Debugf("Expected json: %s", string(exp))
		if string(exp) != string(got) {
			t.Fatal(fmt.Sprintf("Unexpected result %s != %s", string(exp), string(got)))
		}
		for _, s := range expValue.Sensors {
			found := false
			sexp, _ := json.Marshal(s)
			logger.Debugf("Expected json: %s", string(sexp))
			for _, gs := range gotValue.Sensors {
				sgot, _ := json.Marshal(gs)
				if string(sexp) == string(sgot) {
					found = true
				}
			}
			if !found {
				t.Fatal(fmt.Sprintf("Unexpected result %s != %s", string(exp), string(got)))
			}
		}
	}
}
