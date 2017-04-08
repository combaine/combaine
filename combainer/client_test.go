package combainer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/combaine/combaine/common/configs"
	"github.com/stretchr/testify/assert"
)

const repopath = "../tests/testdata/configs"

func TestNewClient(t *testing.T) {
	repo, err := configs.NewFilesystemRepository(repopath)
	assert.Nil(t, err, fmt.Sprintf("Unable to create repo %s", err))

	c, err := NewClient(nil, repo)
	assert.Nil(t, err, fmt.Sprintf("Unable to create client %s", err))
	assert.NotEmpty(t, c.ID)
}

func TestDialContext(t *testing.T) {
	cases := []struct {
		hosts    []string
		expected string
		empty    bool
	}{
		{[]string{}, "empty list of hosts", true},
		{[]string{"host1", "host2", "host3", "host4", "host5", "hosts6"},
			"context deadline exceeded", false},
	}

	for _, c := range cases {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
		gClient, err := dialContext(ctx, c.hosts)
		if c.empty {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), c.expected)
		} else {
			assert.Nil(t, gClient)
			assert.Contains(t, err.Error(), c.expected)
		}
		cancel()
	}
}

func TestUpdateSessionParams(t *testing.T) {
	repo, _ := configs.NewFilesystemRepository(repopath)
	cl, err := NewClient(nil, repo)
	sessionParams, err := cl.updateSessionParams("nop")
	assert.Nil(t, sessionParams)
	assert.Error(t, err)

	sessionParams, err = cl.updateSessionParams("img_status")
	assert.Nil(t, sessionParams)
	assert.Error(t, err)

	var pCfg configs.ParsingConfig
	encPCfg, _ := cl.repository.GetParsingConfig("aggCore")
	assert.NoError(t, encPCfg.Decode(&pCfg))

	sessionParams, err = cl.updateSessionParams("aggCore")
	assert.NoError(t, err)
	assert.Equal(t, len(sessionParams.AggTasks), 1)
	f, err := LoadHostFetcher(nil, pCfg.HostFetcher)
	assert.NoError(t, err, "Faied to load PredefineFetcher")
	predefinedHosts, err := f.Fetch(pCfg.Groups[0])
	t.Log("Fetched hosts", predefinedHosts)
	assert.Equal(t, len(sessionParams.PTasks), len(predefinedHosts["DC1"]))
	assert.Equal(t, sessionParams.ParallelParsings, pCfg.ParallelParsings)
}
