package solomon

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/utils"
	"github.com/stretchr/testify/assert"
)

func init() {
	InitializeLogger(func() logger.Logger {
		logger.CocaineLog = logger.LocalLogger()
		return logger.CocaineLog
	})
}

func TestDumpSensor(t *testing.T) {
	cases := []struct {
		prefix string
		path   utils.NameStack
		value  reflect.Value
		schema []string
		expect *Sensor
	}{
		{"", utils.NameStack{"1", "2", "3", "val"}, reflect.ValueOf(1),
			[]string{"l1", "l2", "l3"},
			&Sensor{
				Labels: map[string]string{"l1": "1", "l2": "2", "l3": "3", "sensor": "val"},
				Value:  float64(1),
				Ts:     0,
			},
		},
		{"p", utils.NameStack{"1", "val"}, reflect.ValueOf(1),
			[]string{"l1"},
			&Sensor{
				Labels: map[string]string{"prefix": "p", "l1": "1", "sensor": "val"},
				Value:  float64(1),
				Ts:     1,
			},
		},
		{"graphiteLikeName", utils.NameStack{"1.2.val"}, reflect.ValueOf(1),
			[]string{"l1", "l2"},
			&Sensor{
				Labels: map[string]string{"prefix": "graphiteLikeName", "l1": "1", "l2": "2", "sensor": "val"},
				Value:  float64(1),
				Ts:     2,
			},
		},
		{"ERROR", utils.NameStack{"1", "val"}, reflect.ValueOf(1),
			[]string{"l1", "l2", "too many schema"},
			nil,
		},
	}

	for i, c := range cases {
		s := Sender{Config: Config{}, id: "TEST", prefix: c.prefix, Schema: c.schema}
		t.Logf("Case %d, %#v", i, s)
		sensor, err := s.dumpSensor(c.path, c.value, uint64(i))
		if c.prefix != "ERROR" {
			assert.NoError(t, err)
		}
		assert.Equal(t, c.expect, sensor)
	}
}

func TestNewWorker(t *testing.T) {
	w := newWorker(0, 0, 0)
	assert.Equal(t, 0, w.id)
	assert.Equal(t, 0, w.Retry)
	assert.Equal(t, time.Duration(0), w.RetryInterval)
}

func TestNewSender(t *testing.T) {
	scl, _ := NewSender(Config{API: "api.addr"}, "id")
	assert.Equal(t, scl.id, "id")
	assert.Equal(t, scl.API, "api.addr")
}

func TestStartWorkers(t *testing.T) {
	j := make(chan Job, 1)
	StartWorkers(j, 5)

	j <- Job{PushData: []byte{}, SolCli: &Sender{Config: Config{API: "bad://proto"}}}
	for i := 6; i > 0; i-- {
		if len(j) == 0 {
			close(j)
			break
		}
		time.Sleep(time.Millisecond)
		runtime.Gosched()
	}
	assert.Equal(t, 0, len(j))
}
func TestRequest(t *testing.T) {
	// for network timeout test
	uriWithoutListener := "http://127.0.9.1:35333"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/200":
			fmt.Fprintln(w, "200")
		case "/timeout":
			w.WriteHeader(200)
			time.Sleep(10 * time.Millisecond)
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
		{Job{PushData: []byte{}, SolCli: &Sender{}},
			"worker 0 failed to send after 0 attempts, dropping job", 0, true},
		{Job{PushData: []byte{}, SolCli: &Sender{
			Config: Config{API: "://bad_url", Timeout: 10}}},
			"parse ://bad_url: missing protocol scheme", 1, true},
		{Job{PushData: []byte{}, SolCli: &Sender{
			Config: Config{API: uriWithoutListener, Timeout: 10}}},
			"connection refused", 1, true},

		{Job{PushData: []byte{}, SolCli: &Sender{
			Config: Config{API: ts.URL + "/timeout", Timeout: 5}}},
			"worker 3 failed to send after 8 attempts, dropping job", 8, false},

		{Job{PushData: []byte{}, SolCli: &Sender{
			Config: Config{API: ts.URL + "/200", Timeout: 10}}},
			"worker 4 failed to send after 2 attempts, dropping job", 2, false},
		{Job{PushData: []byte{}, SolCli: &Sender{
			Config: Config{API: ts.URL + "/404", Timeout: 10}}},
			"404 Not Found", 3, true},
		{Job{PushData: []byte{}, SolCli: &Sender{
			Config: Config{API: ts.URL + "/408", Timeout: 10}}},
			"worker 6 failed to send after 3 attempts, dropping job", 3, true},
	}

	for i, c := range cases {
		w := newWorker(i, c.attempt, 3)
		err := w.sendToSolomon(c.job)
		t.Log(err)
		if err != nil {
			assert.Contains(t, fmt.Sprintf("%v", err), c.expected)
		} else {
			if c.err {
				t.Fatal("expect error")
			}
		}
	}
}

func TestSolomonClientSendNil(t *testing.T) {
	dropJobs(JobQueue)
	cli, _ := NewSender(Config{API: "api.addr"}, "id")

	// bad task
	task := []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
			Result: []int{}},
	}
	err := cli.Send(task, 111)
	assert.Contains(t, fmt.Sprintf("%v", err), "Value of group should be dict")

	// empty task
	task = []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
			Result: make(map[string]interface{})},
	}
	err = cli.Send(task, 111)
	assert.NoError(t, err)

	err = cli.Send(nil, 111)
	assert.Contains(t, fmt.Sprintf("%v", err), "Nothing to send")

	assert.Equal(t, 0, len(JobQueue))
}

func TestSolomonClientSendArray(t *testing.T) {
	dropJobs(JobQueue)
	cli, _ := NewSender(Config{}, "_id")

	task := []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
			Result: map[string]interface{}{"ArrOv": []interface{}{20, 30.0}}},
	}
	err := cli.Send(task, 111)
	assert.Contains(t, fmt.Sprintf("%v", err), "Unable to send a slice. Fields len 0")
	assert.Equal(t, 0, len(JobQueue))

	cli, _ = NewSender(Config{Fields: []string{"1f", "2f"}}, "_id")
	err = cli.Send(task, 111)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(JobQueue))

	task = []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
			Result: map[string]interface{}{"ArrOv": []interface{}{"20", uint(30)}}},
	}
	// client expect number as string
	err = cli.Send(task, 111)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(JobQueue))
}

func TestSolomonClientSendNotANumber(t *testing.T) {
	dropJobs(JobQueue)
	cases := []struct {
		task     []common.AggregationResult
		expected string
	}{
		// hmm, actually sender cannot parse not common types of numbers
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
				Result: map[string]interface{}{"map": map[string]interface{}{"MAPMAP1": map[string]interface{}{"MoMv1": true}}}}},
			"Not a Number"},
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
				Result: map[interface{}]interface{}{nil: []string{"test", "test"}}}},
			`strconv.ParseFloat: parsing "test": invalid syntax: test`},
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
				Result: map[interface{}]interface{}{nil: map[string]bool{"test": false}}}},
			"Not a Number"},
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "host", "name": "grp", "metahost": "grp", "aggregate": "app"},
				Result: map[interface{}]interface{}{nil: nil}}},
			"Not a Number"},
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "datacenter", "name": "grp", "metahost": "grp", "aggregate": "app"},
				Result: map[interface{}]interface{}{nil: nil}}},
			"Not a Number"},
		{[]common.AggregationResult{
			{Tags: map[string]string{"aggregate": "app"},
				Result: map[interface{}]interface{}{nil: nil}}},
			"Failed to get data tag 'name'"},
		{[]common.AggregationResult{
			{Tags: map[string]string{"name": "grp", "aggregate": "app"},
				Result: map[interface{}]interface{}{nil: nil}}},
			"Failed to get data tag 'type'"},
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "datacenter", "name": "grp", "aggregate": "app"},
				Result: map[interface{}]interface{}{nil: nil}}},
			"Failed to get data tag 'metahost'"},
	}
	cli, _ := NewSender(Config{API: "api.addr", Fields: []string{"1_prc", "2_prc"}}, "id")

	for _, c := range cases {
		assert.NotPanics(t, func() {
			assert.Contains(t, fmt.Sprintf("%v", cli.Send(c.task, 111)), c.expected)
		})
	}
	assert.Equal(t, 0, len(JobQueue))
}

func TestSolomonInternalSend(t *testing.T) {

	var (
		ts           uint64
		expectedData map[string]solomonPush
	)

	solCfg := Sender{
		Config: Config{Project: "combaine", Cluster: "backend"},
		id:     "testId",
	}

	simpleOneItemData := []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "hostName", "metahost": "hostName", "aggregate": "serviceName.With.Dot"},
			Result: map[string]interface{}{"sensorName": 17}},
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
	data, err := solCfg.sendInternal(simpleOneItemData, ts)
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
		t.Logf("Expected json: %s", string(exp))
		t.Logf("Resulted json: %s", string(got))
		if string(exp) != string(got) {
			t.Fatal(fmt.Sprintf("Unexpected result %s != %s", string(exp), string(got)))
		}
	}

	// Complex test
	solCfg = Sender{
		Config: Config{
			Project: "TEST",
			Cluster: "TESTCOMBAINE",
			Fields:  []string{"25_prc", "50_prc", "99_prc"},
		},
		id: "TESTID",
	}
	app1Data := []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "group1", "metahost": "group1", "aggregate": "app1"},
			Result: map[string]interface{}{"ArrayOv": []int{20, 30, 40}}},
		{Tags: map[string]string{"type": "host", "name": "group2", "metahost": "group2", "aggregate": "app1"},
			Result: map[string]interface{}{"map_of_array": map[string]interface{}{
				"MoAv1": []interface{}{201, 301, 401},
				"MoAv2": []interface{}{202, 302, 402},
			}}},
		{Tags: map[string]string{"type": "host", "name": "host4", "metahost": "host4", "aggregate": "app1"},
			Result: map[string]interface{}{"mapOfSimple": map[string]interface{}{"MoSv1": 1001, "MoSv2": 1002}}},
		{Tags: map[string]string{"type": "host", "name": "host5", "metahost": "host5", "aggregate": "app1"},
			Result: map[string]interface{}{
				"map_of_map": map[string]interface{}{
					"MAPMAP1": map[string]interface{}{"MoMv1": 76, "MoMv2": 77},
					"MAPMAP2": map[string]interface{}{"MoMv1": 87, "MoMv2": 88},
				}}},
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
	data, err = solCfg.sendInternal(app1Data, ts)
	if err != nil {
		t.Fatal(err)
	}
	for _, gotValue := range data {
		expValue := expectedData[gotValue.CommonLabels.Host]
		assert.Equal(t, len(expValue.Sensors), len(gotValue.Sensors))
		exp, _ := json.Marshal(expValue.CommonLabels)
		got, _ := json.Marshal(gotValue.CommonLabels)
		t.Logf("Expected json: %s", string(exp))
		t.Logf("Resulted json: %s", string(got))
		if string(exp) != string(got) {
			t.Fatal(fmt.Sprintf("Unexpected result %s != %s", string(exp), string(got)))
		}
		for _, s := range expValue.Sensors {
			found := false
			sexp, _ := json.Marshal(s)
			t.Logf("Expected json: %s", string(sexp))
			for _, gs := range gotValue.Sensors {
				sgot, _ := json.Marshal(gs)
				if string(sexp) == string(sgot) {
					t.Logf("Resulted json: %s", string(sgot))
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
