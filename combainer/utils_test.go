package combainer

import (
	"testing"
	"time"

	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/tests"
	"github.com/stretchr/testify/assert"
)

func TestGenSessionId(t *testing.T) {
	id1 := GenerateSessionId()
	id2 := GenerateSessionId()
	t.Logf("%s %s", id1, id2)
	if id1 == id2 {
		t.Fatal("the same id")
	}
}

func BenchmarkGenSessionId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateSessionId()
	}
}

func TestGenerateSessionTimeFrame(t *testing.T) {
	parsingTime, wholeTime := GenerateSessionTimeFrame(10)
	assert.Equal(t, parsingTime, time.Duration(8*time.Second))
	assert.Equal(t, wholeTime, time.Duration(10*time.Second))
}

func TestGetAggregationConfigs(t *testing.T) {
	// repo configs.Repository, parsingConfig *configs.ParsingConfig) (*map[string]configs.AggregationConfig, error) {
	repo := tests.NewRepo([]string{})
	cfg, _ := repo.GetParsingConfig("")
	var parsingConfig configs.ParsingConfig
	assert.NoError(t, cfg.Decode(&parsingConfig))
	parsingConfig.AggConfigs = []string{"test"}
	aggCfgs, err := GetAggregationConfigs(repo, &parsingConfig)
	assert.NotNil(t, (*aggCfgs)["test"])
	assert.Len(t, *aggCfgs, 1)
	assert.NoError(t, err)
}
