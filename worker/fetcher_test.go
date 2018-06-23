package worker

import (
	"context"
	"testing"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/stretchr/testify/assert"
)

var fch = make(chan string, 2) // do not block fetcher

func NewDummyFetcher(cfg repository.PluginConfig) (Fetcher, error) {
	return &fether{c: cfg}, nil
}

type fether struct {
	c repository.PluginConfig
}

func (f *fether) Fetch(_ context.Context, task *common.FetcherTask) ([]byte, error) {
	fch <- string(f.c["timetail_url"].([]byte))
	return common.Pack(*task)
}

func NewTestFetcher(_ repository.PluginConfig) (Fetcher, error) {
	return &testFether{}, nil
}

type testFether struct{}

func (f *testFether) Fetch(_ context.Context, _ *common.FetcherTask) ([]byte, error) {
	return nil, nil
}

func TestFetch(t *testing.T) {
	Register("test", NewTestFetcher)
	t.Log("test fetcher registered")

	_, err := NewFetcher("nonExisting"+time.Now().String(), repository.PluginConfig{})
	assert.Error(t, err)
	_, err = NewFetcher("test", repository.PluginConfig{})
	assert.NoError(t, err)
}
