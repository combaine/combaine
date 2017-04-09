package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAggregationConfigs(t *testing.T) {
	const repopath = "../tests/testdata/configs"
	repo, repoErr := NewFilesystemRepository(repopath)
	assert.NoError(t, repoErr)

	cfg, _ := repo.GetParsingConfig("aggCore")
	var parsingConfig ParsingConfig
	assert.NoError(t, cfg.Decode(&parsingConfig))
	aggCfgs, err := GetAggregationConfigs(repo, &parsingConfig)
	assert.NotNil(t, (*aggCfgs)["aggCore"])
	assert.Len(t, *aggCfgs, 1)
	assert.NoError(t, err)
}
