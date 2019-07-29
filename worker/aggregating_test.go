package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/combaine/combaine/repository"
)

const cfgName = "aggCore"

var (
	pcfg              repository.EncodedConfig
	acfg              repository.EncodedConfig
	parsingConfig     repository.ParsingConfig
	aggregationConfig repository.AggregationConfig
)

func TestInit(t *testing.T) {
	var err error

	pcfg, err = repository.GetParsingConfig(cfgName)
	assert.NoError(t, err, "unable to read parsingCfg %s: %s", cfgName, err)
	assert.NoError(t, pcfg.Decode(&parsingConfig))

	acfg, err = repository.GetAggregationConfig(cfgName)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", cfgName, err)
	assert.NoError(t, acfg.Decode(&aggregationConfig))
}

func TestAggregating(t *testing.T) {
}
