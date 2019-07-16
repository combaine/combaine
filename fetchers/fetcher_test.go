package fetchers

import (
	"context"
	"testing"

	"github.com/combaine/combaine/repository"
	"github.com/stretchr/testify/assert"
)

var testFetcherName = "testFetcher"

type testFetcher struct{}

func (f *testFetcher) Fetch(ctx context.Context, task *FetcherTask) ([]byte, error) {
	return []byte(testFetcherName), nil
}

func newTestFetcher(cfg repository.PluginConfig) (Fetcher, error) {
	return &testFetcher{}, nil
}

func TestRegisterFetcher(t *testing.T) {
	Register(testFetcherName, newTestFetcher)
	fLock.Lock()
	defer fLock.Unlock()
	_, ok := fetchers[testFetcherName]
	assert.True(t, ok)
}

func TestNewFetcher(t *testing.T) {
	_, err := NewFetcher("not-registered", nil)
	assert.Error(t, err)

	Register(testFetcherName, newTestFetcher)
	f, err := NewFetcher(testFetcherName, nil)
	assert.NoError(t, err)
	data, _ := f.Fetch(context.TODO(), nil)
	assert.Equal(t, testFetcherName, string(data))
}
