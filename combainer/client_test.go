package combainer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/stretchr/testify/assert"
)

const repopath = "../tests/testdata/configs"

var repo, repoErr = common.NewFilesystemRepository(repopath)

func TestNewClient(t *testing.T) {
	assert.Nil(t, repoErr, fmt.Sprintf("Unable to create repo %s", repoErr))

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
	cl, err := NewClient(nil, repo)
	sessionParams, err := cl.updateSessionParams("nop")
	assert.Nil(t, sessionParams)
	assert.Error(t, err)

	sessionParams, err = cl.updateSessionParams("img_status")
	assert.Nil(t, sessionParams)
	assert.Error(t, err)

	var pCfg common.ParsingConfig
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

func TestGenerateSessionTimeFrame(t *testing.T) {
	parsingTime, wholeTime := generateSessionTimeFrame(10)
	assert.Equal(t, parsingTime, time.Duration(8*time.Second))
	assert.Equal(t, wholeTime, time.Duration(10*time.Second))
}
