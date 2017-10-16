package worker

import (
	"testing"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/stretchr/testify/assert"
)

var fch = make(chan string, 2) // do not block fetcher

func NewDummyFetcher(cfg common.PluginConfig) (Fetcher, error) {
	return &fether{c: cfg}, nil
}

type fether struct {
	c common.PluginConfig
}

func (f *fether) Fetch(task *common.FetcherTask) ([]byte, error) {
	fch <- string(f.c["timetail_url"].([]byte))
	return common.Pack(*task)
}

func NewTestFetcher(_ common.PluginConfig) (Fetcher, error) {
	return &testFether{}, nil
}

type testFether struct{}

func (f *testFether) Fetch(_ *common.FetcherTask) ([]byte, error) {
	return nil, nil
}

func TestFetch(t *testing.T) {
	Register("test", NewTestFetcher)
	t.Log("test fetcher registered")

	_, err := NewFetcher("nonExisting"+time.Now().String(), common.PluginConfig{})
	assert.Error(t, err)
	_, err = NewFetcher("test", common.PluginConfig{})
	assert.NoError(t, err)
}
