package agave

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	InitializeLogger(func() logger.Logger {
		logger.CocaineLog = logger.LocalLogger()
		return logger.CocaineLog
	})
}

func TestSend(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "200")
	}))
	defer ts.Close()

	testConfig := Config{
		Items: []string{
			"20x",
			"20x.MAP1",
			"20x.MP2",
			"20x.MP3",
			"30x",
		},
		Hosts:         []string{"localhost", ts.Listener.Addr().String()},
		GraphName:     "GraphName",
		GraphTemplate: "graph_template",
		Fields:        []string{"A", "B", "C"},
		Step:          300,
	}
	s, err := NewSender("testID", testConfig)

	assert.NoError(t, err)

	data := []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "host1", "metahost": "host1", "aggregate": "20x"},
			Result: 2000},
		{Tags: map[string]string{"type": "datacenter", "name": "DC1", "metahost": "host3", "aggregate": "20x"},
			Result: map[string]interface{}{
				"MAP1": []interface{}{201, 301, 401},
				"MAP2": []interface{}{202, 302, 402},
			}},
		{Tags: map[string]string{"type": "host", "name": "host4", "metahost": "host4", "aggregate": "20x"},
			Result: map[string]interface{}{"MP1": 1000, "MP2": 1002}},
		{Tags: map[string]string{"type": "metahost", "name": "host2", "metahost": "host2", "aggregate": "30x"},
			Result: []int{20, 30, 40}},
		{Tags: map[string]string{"type": "metahost", "name": "host4", "metahost": "host4", "aggregate": "30x"},
			Result: map[string]interface{}{"MP1": 1000, "MP2": 1002}},
	}

	expected := map[string][]string{
		"host1":     {"20x:2000"},
		"host3-DC1": {"A:201+B:301+C:401"},
		"host4":     {"MP2:1002"},
		"host2":     {"A:20+B:30+C:40"},
	}

	actual, err := s.send(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)

	err = s.Send(context.Background(), data)
	assert.NoError(t, err)
}

func TestSendFailed(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	defer ts.Close()

	testConfig := Config{
		Items:         []string{"20x"},
		Hosts:         []string{ts.Listener.Addr().String()},
		GraphName:     "GraphName",
		GraphTemplate: "graph_template",
		Step:          300,
	}
	s, err := NewSender("testID", testConfig)

	assert.NoError(t, err)

	data := []common.AggregationResult{
		{Tags: map[string]string{"type": "host", "name": "host1", "metahost": "host1", "aggregate": "20x"},
			Result: 2000},
	}

	ctx := context.Background()
	err = s.Send(ctx, data)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "Timeout")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	err = s.Send(ctx, data)
	cancel()
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), context.DeadlineExceeded.Error())
	}
}
