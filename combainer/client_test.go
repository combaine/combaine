package combainer

import (
	"fmt"
	"testing"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	c, err := NewClient()
	assert.Nil(t, err, fmt.Sprintf("Unable to create client %s", err))
	assert.NotEmpty(t, c.ID)
	c.Close()
}

func TestUpdateSessionParams(t *testing.T) {
	cl, err := NewClient()
	sessionParams, err := cl.updateSessionParams("nop")
	assert.Nil(t, sessionParams)
	assert.Error(t, err)

	sessionParams, err = cl.updateSessionParams("img_status")
	assert.Nil(t, sessionParams)
	assert.Error(t, err)

	var pCfg repository.ParsingConfig
	encPCfg, _ := repository.GetParsingConfig("aggCore")
	assert.NoError(t, encPCfg.Decode(&pCfg))

	sessionParams, err = cl.updateSessionParams("aggCore")
	assert.NoError(t, err)
	assert.Equal(t, len(sessionParams.AggTasks), 1)
	f, err := common.LoadHostFetcher(pCfg.HostFetcher)
	assert.NoError(t, err, "Faied to load PredefineFetcher")
	predefinedHosts, err := f.Fetch(pCfg.Groups[0])
	t.Log("Fetched hosts", predefinedHosts)
	assert.Equal(t, len(sessionParams.PTasks), len(predefinedHosts["DC1"]))
	assert.Equal(t, sessionParams.ParallelParsings, pCfg.ParallelParsings)
	assert.False(t, sessionParams.aggregateLocally)
}

func TestGenerateSessionTimeFrame(t *testing.T) {
	parsingTime, wholeTime := generateSessionTimeFrame(10)
	assert.Equal(t, parsingTime, time.Duration(7*time.Second))
	assert.Equal(t, wholeTime, time.Duration(10*time.Second))
}
