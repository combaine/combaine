package solomon

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
	"github.com/stretchr/testify/assert"
)

func TestNewWorker(t *testing.T) {
	w := NewWorker(0, 0)
	assert.Equal(t, 0, w.Id)
	assert.Equal(t, 0, w.Retry)
}

func TestNewSolomonClient(t *testing.T) {
	cli, _ := NewSolomonClient(SolomonCfg{Api: "api.addr"}, "id")
	scl := cli.(*solomonClient)
	assert.Equal(t, scl.id, "id")
	assert.Equal(t, scl.Api, "api.addr")
}

func TestStartWorkers(t *testing.T) {
	j := make(chan Job, 1)
	StartWorkers(j)

	j <- Job{PushData: []byte{}, SolCli: &solomonClient{SolomonCfg: SolomonCfg{Api: "bad://proto"}}}
	for i := 3; i > 0; i-- {
		if len(j) == 0 {
			close(j)
			break
		}
		runtime.Gosched()
	}
	assert.Equal(t, 0, len(j))
}
func TestRequest(t *testing.T) {
	netAddr := "127.0.0.1:35313"

	// for network timeout test
	l, _ := net.Listen("tcp", netAddr)
	defer l.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/200":
			fmt.Fprintln(w, "200")
		case "/408":
			w.WriteHeader(408)
			fmt.Fprintln(w, "408")
		case "/404":
			w.WriteHeader(404)
			fmt.Fprintln(w, "404")
		}
	}))
	defer ts.Close()

	cases := []struct {
		job      Job
		expected string
		attempt  int
		err      bool
	}{
		{Job{PushData: []byte{}, SolCli: &solomonClient{}},
			"worker 0 failed to send after 0 attempts, dropping job", 0, true},
		{Job{PushData: []byte{}, SolCli: &solomonClient{
			SolomonCfg: SolomonCfg{Api: "://bad_url", Timeout: 10}}},
			"parse ://bad_url: missing protocol scheme", 1, true},
		{Job{PushData: []byte{}, SolCli: &solomonClient{
			SolomonCfg: SolomonCfg{Api: "http://127.0.0.1:35333", Timeout: 10}}},
			"getsockopt: connection refused", 1, true},

		{Job{PushData: []byte{}, SolCli: &solomonClient{
			SolomonCfg: SolomonCfg{Api: ts.URL + "/200", Timeout: 50}}},
			"worker 3 failed to send after 2 attempts, dropping job", 2, false},
		{Job{PushData: []byte{}, SolCli: &solomonClient{
			SolomonCfg: SolomonCfg{Api: ts.URL + "/404", Timeout: 50}}},
			"404 Not Found", 3, true},
		{Job{PushData: []byte{}, SolCli: &solomonClient{
			SolomonCfg: SolomonCfg{Api: ts.URL + "/408", Timeout: 20}}},
			"worker 5 failed to send after 3 attempts, dropping job", 3, true},
	}

	for i, c := range cases {
		w := NewWorker(i, c.attempt)
		err := w.SendToSolomon(c.job)
		t.Log(err)
		if err != nil {
			assert.Contains(t, err.Error(), c.expected)
		} else {
			if c.err {
				t.Fatal("expect error")
			}
		}
	}
}

func TestSolomonClientSendNil(t *testing.T) {
	dropJobs(JobQueue)
	cli, _ := NewSolomonClient(SolomonCfg{Api: "api.addr"}, "id")

	// empty task
	task := tasks.DataType{"app": {"grp": nil}}
	err := cli.Send(task, 111)
	assert.Contains(t, err.Error(), "Value of group should be dict")

	err = cli.Send(nil, 111)
	assert.Contains(t, err.Error(), "Nothing to send")

	assert.Equal(t, 0, len(JobQueue))
}

func TestSolomonClientSendArray(t *testing.T) {
	dropJobs(JobQueue)
	cli, _ := NewSolomonClient(SolomonCfg{}, "_id")

	task := tasks.DataType{
		"app": {"grp": map[string]interface{}{"ArrOv": []interface{}{20, 30.0}}},
	}
	err := cli.Send(task, 111)
	assert.Contains(t, err.Error(), "Unable to send a slice. Fields len 0")
	assert.Equal(t, 0, len(JobQueue))

	cli, _ = NewSolomonClient(SolomonCfg{Fields: []string{"1f", "2f"}}, "_id")
	err = cli.Send(task, 111)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(JobQueue))

	task = tasks.DataType{
		"app": {"grp": map[string]interface{}{"ArrOv": []interface{}{"20", uint(30)}}},
	}
	// client expect number as string
	err = cli.Send(task, 111)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(JobQueue))
}

func TestSolomonClientSendNotANumber(t *testing.T) {
	dropJobs(JobQueue)
	cases := []struct {
		task     tasks.DataType
		expected string
	}{
		// hmm, actually sender cannot parse not common types of numbers
		{tasks.DataType{"app": {"grp": map[string]interface{}{
			"map": map[string]interface{}{"MAPMAP1": map[string]interface{}{"MoMv1": true}}}}},
			"Not a Number"},
		{tasks.DataType{"app": {"grp": map[interface{}]interface{}{nil: []string{"test", "test"}}}},
			`strconv.ParseFloat: parsing "test": invalid syntax: test`},
		{tasks.DataType{"app": {"grp": map[interface{}]interface{}{nil: map[string]bool{"test": false}}}},
			"Not a Number"},
		{tasks.DataType{"app": {"grp": map[interface{}]interface{}{nil: nil}}},
			"Not a Number"},
	}
	cli, _ := NewSolomonClient(SolomonCfg{Api: "api.addr", Fields: []string{"1_prc", "2_prc"}}, "id")

	for _, c := range cases {
		assert.NotPanics(t, func() {
			assert.Contains(t, cli.Send(c.task, 111).Error(), c.expected)
		})
	}
	assert.Equal(t, 0, len(JobQueue))
}

func TestSolomonInternalSend(t *testing.T) {

	var (
		ts           uint64
		expectedData map[string]solomonPush
	)

	solCfg := solomonClient{
		SolomonCfg: SolomonCfg{Project: "combaine", Cluster: "backend"},
		id:         "testId",
	}

	simpleOneItemData := tasks.DataType{
		"serviceName.With.Dot": {"hostName": map[string]interface{}{"sensorName": 17}},
	}

	simpleOneItemJSON := `{
		"hostName": {"commonLabels": {
			"project": "combaine",
			"cluster": "backend",
			"host":	   "hostName",
			"service": "serviceName"
		},
		"sensors": [{"labels": {"sensor": "With.Dot.sensorName"}, "ts": 10, "value": 17}]}
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
		SolomonCfg: SolomonCfg{
			Project: "TEST",
			Cluster: "TESTCOMBAINE",
			Fields:  []string{"25_prc", "50_prc", "99_prc"},
		},
		id: "TESTID",
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
				"mapOfSimple": map[string]interface{}{"MoSv1": 1001, "MoSv2": 1002},
			},
			"host5": map[string]interface{}{
				"map_of_map": map[string]interface{}{
					"MAPMAP1": map[string]interface{}{"MoMv1": 76, "MoMv2": 77},
					"MAPMAP2": map[string]interface{}{"MoMv1": 87, "MoMv2": 88},
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
			{"labels": {"sensor": "mapOfSimple.MoSv1"}, "ts": 200, "value": 1001},
			{"labels": {"sensor": "mapOfSimple.MoSv2"}, "ts": 200, "value": 1002}
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

func dropJobs(j chan Job) {
END:
	for {
		select {
		case _, ok := <-j:
			if !ok {
				break END
			}
		default:
			break END
		}
	}
}
