package graphite

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/stretchr/testify/assert"
)

func init() {
	InitializeLogger(func() logger.Logger {
		logger.CocaineLog = logger.LocalLogger()
		return logger.CocaineLog
	})
}

type ioWriteFailerCloser struct {
	counter int
}

func (*ioWriteFailerCloser) Write(s []byte) (int, error) {
	return -1, fmt.Errorf("testingErr")
}
func (*ioWriteFailerCloser) Close() error {
	return nil
}

func testtcp(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "")
	if err != nil {
		t.Fatalf(err.Error())
	}
	go func() {
		for {
			_, err := l.Accept()
			if err != nil {
				return
			}
		}
	}()
	return l
}

func TestNewConn(t *testing.T) {
	cases := []struct {
		args        []interface{}
		expected    string
		shouldError bool
	}{
		{[]interface{}{}, "Not enought arguments", true},
		{[]interface{}{"one"}, "Not enought arguments", true},
		{[]interface{}{"one", "two"}, "Failed to parse arguments retry or timeout", true},
		{[]interface{}{1, 20}, "successfully connected", false},
	}

	for _, c := range cases {
		var err error
		var l net.Listener
		if !c.shouldError {
			l = testtcp(t)
			t.Logf("work with addr %s", l.Addr().String())
			_, err = NewConn(l.Addr().String(), c.args...)
		} else {
			_, err = NewConn("", c.args...)
		}

		if c.shouldError {
			assert.Contains(t, fmt.Sprintf("%v", err), c.expected)
		} else {
			assert.NoError(t, err)
		}

		if l != nil {
			l.Close()
		}
	}
}

func TestCacheGetEvict(t *testing.T) {
	var counter int
	c := NewCacher(func(s string, a ...interface{}) (io.WriteCloser, error) {
		counter++
		return &ioWriteFailerCloser{counter}, nil
	})

	f1, _ := c.Get("name")
	_, _ = c.Get("name")
	assert.Equal(t, 1, len(c.(*cacher).cache))
	c.Evict(f1)
	assert.Equal(t, 0, len(c.(*cacher).cache))

	s1, _ := c.Get("name")
	s2, _ := c.Get("name")
	assert.Equal(t, s1.(*ioWriteFailerCloser).counter, s2.(*ioWriteFailerCloser).counter)
	assert.Equal(t, 1, len(c.(*cacher).cache))
	c.Evict(s2)
	assert.Equal(t, 0, len(c.(*cacher).cache))

	s3, _ := c.Get("name")
	assert.NotEqual(t, s2.(*ioWriteFailerCloser).counter, s3.(*ioWriteFailerCloser).counter)

}

func TestNewGraphiteClient(t *testing.T) {
	gSender, err := NewSender(&Config{}, "id")
	assert.NoError(t, err)
	assert.Equal(t, gSender.id, "id")
}

func TestGraphiteSend(t *testing.T) {
	grCfg := Sender{
		id:      "TESTID",
		cluster: "TESTCOMBAINE",
		fields:  []string{"A", "B", "C"},
	}

	cases := []struct {
		data     []common.AggregationResult
		expected []string
	}{
		{
			[]common.AggregationResult{
				{Tags: map[string]string{"type": "host", "name": "simple", "metahost": "simple", "aggregate": "20x"},
					Result: 2000},
				{Tags: map[string]string{
					"type": "host", "name": "array", "metahost": "array", "aggregate": "20x"},
					Result: []int{20, 30, 40}},
				{Tags: map[string]string{
					"type": "datacenter", "name": "DC1", "metahost": "map_of_array", "aggregate": "20x"},
					Result: map[string]interface{}{
						"MAP1": []interface{}{201, 301, 401},
						"MAP2": []interface{}{202, 302, 402}},
				}},
			[]string{"TESTCOMBAINE.combaine.simple.20x 2000",
				"TESTCOMBAINE.combaine.array.20x.A 20",
				"TESTCOMBAINE.combaine.array.20x.B 30",
				"TESTCOMBAINE.combaine.array.20x.C 40",
				"TESTCOMBAINE.combaine.map_of_array_DC1.20x.MAP1.A 201",
				"TESTCOMBAINE.combaine.map_of_array_DC1.20x.MAP1.B 301",
				"TESTCOMBAINE.combaine.map_of_array_DC1.20x.MAP1.C 401",
				"TESTCOMBAINE.combaine.map_of_array_DC1.20x.MAP2.A 202",
				"TESTCOMBAINE.combaine.map_of_array_DC1.20x.MAP2.B 302",
				"TESTCOMBAINE.combaine.map_of_array_DC1.20x.MAP2.C 402"},
		},
		{
			[]common.AggregationResult{
				{Tags: map[string]string{
					"type": "host", "name": "map_of_simple", "metahost": "map_of_simple", "aggregate": "20x"},
					Result: map[string]interface{}{
						"MP1": 1000,
						"MP2": 1002},
				}},
			[]string{"TESTCOMBAINE.combaine.map_of_simple.20x.MP1 1000",
				"TESTCOMBAINE.combaine.map_of_simple.20x.MP2 1002"},
		},
		{
			[]common.AggregationResult{
				{Tags: map[string]string{
					"type": "host", "name": "map_of_map", "metahost": "map_of_map", "aggregate": "20x"},
					Result: map[string]interface{}{
						"MAPMAP1": map[string]interface{}{
							"MPMP1": 1000,
							"MPMP2": 1002}},
				}},
			[]string{"TESTCOMBAINE.combaine.map_of_map.20x.MAPMAP1.MPMP1 1000",
				"TESTCOMBAINE.combaine.map_of_map.20x.MAPMAP1.MPMP2 1002"},
		},
		{
			[]common.AggregationResult{{Tags: map[string]string{
				"type": "host", "name": "simple", "metahost": "simple", "aggregate": "30x"},
				Result: 2000,
			}},
			[]string{"TESTCOMBAINE.combaine.simple.30x 2000"},
		},
	}

	buff := new(bytes.Buffer)
	for i, c := range cases {
		err := grCfg.sendInternal(c.data, uint64(i), buff)
		assert.NoError(t, err)
		result := "\n" + buff.String() + "\n"

		for _, item := range c.expected {
			expectedItem := fmt.Sprintf("%s %d", item, i)
			assert.Contains(t, result, expectedItem)
		}
	}
}

func TestGraphiteSendError(t *testing.T) {
	grCfg := Sender{}
	ioWErr := new(ioWriteFailerCloser)

	cases := []struct {
		data     []common.AggregationResult
		expected string
	}{
		{
			[]common.AggregationResult{
				{Tags: map[string]string{
					"type": "host", "name": "array", "metahost": "array", "aggregate": "20x"},
					Result: []int{20, 30, 40},
				}},
			"Unable to send a slice. Fields len 0, len of value 3",
		},
		{
			[]common.AggregationResult{
				{Tags: map[string]string{
					"type": "host", "name": "map_of_array", "metahost": "map_of_array", "aggregate": "20x"},
					Result: map[string]interface{}{"MAP2": []interface{}{202}},
				}},
			"Unable to send a slice. Fields len 0, len of value 1",
		},
	}

	for _, c := range cases {
		err := grCfg.sendInternal(c.data, 1, new(bytes.Buffer))
		assert.Error(t, err)
		assert.Contains(t, fmt.Sprintf("%v", err), c.expected)
	}

	assert.Error(t, grCfg.send(ioWErr, "test"))
	assert.Contains(t, grCfg.send(ioWErr, "test").Error(), "testingErr")

	grCfg = Sender{fields: []string{"A"}}

	data := []common.AggregationResult{
		{Tags: map[string]string{
			"type": "host", "name": "arr", "metahost": "arr", "aggregate": "20x"},
			Result: []int{20},
		}}
	err := grCfg.sendInternal(data, 1, ioWErr)
	assert.Error(t, err, "test")
	assert.Contains(t, fmt.Sprintf("%v", err), "testingErr")

}

func TestNetSend(t *testing.T) {
	l := testtcp(t)
	defer l.Close()
	t.Logf("work with addr %s", l.Addr().String())

	gc := Sender{id: "TESTID", endpoint: l.Addr().String()}

	cases := []struct {
		data     []common.AggregationResult
		expected string
		withErr  bool
		fields   []string
	}{
		{[]common.AggregationResult{}, "Empty data. Nothing to send", true, []string{}},
		{
			[]common.AggregationResult{
				{Tags: map[string]string{
					"type": "host", "name": "array", "metahost": "array", "aggregate": "20x"},
					Result: []int{20, 30, 40},
				}},
			"TESTID Unable to send a slice. Fields len 0, len of value 3", true, []string{},
		},
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "datacenter", "name": "DC1", "aggregate": "20x"}, Result: []int{0}}},
			"Failed to get data tag 'metahost', skip task", false, []string{"A"}},
		{[]common.AggregationResult{
			{Tags: map[string]string{"type": "datacenter", "metahost": "array", "aggregate": "20x"}, Result: []int{0}}},
			"Failed to get data tag 'metahost', skip task", false, []string{"A"}},
		{[]common.AggregationResult{
			{Tags: map[string]string{"metahost": "array", "name": "DC1", "aggregate": "20x"}, Result: []int{0}}},
			"Failed to get data tag 'metahost', skip task", false, []string{"A"}},
	}

	for _, c := range cases {
		gc.fields = c.fields
		err := gc.Send(c.data, 1)
		if c.withErr {
			assert.Error(t, err)
			assert.Contains(t, fmt.Sprintf("%v", err), c.expected)
		} else {
			assert.Nil(t, err)
		}
	}

	gc = Sender{endpoint: ":::port"}
	err := gc.Send(cases[1].data, 1)
	assert.Error(t, err)
	assert.Contains(t, fmt.Sprintf("%v", err), "too many colons in address")

	gc = Sender{endpoint: ":10101"}
	connectionTimeout = -1
	reconnectInterval = 5
	err = gc.Send(cases[1].data, 3)
	assert.Error(t, err)

	gc = Sender{fields: []string{"A", "B", "C"}, endpoint: l.Addr().String()}
	err = gc.Send(cases[1].data, 1)
	assert.NoError(t, err)
}
