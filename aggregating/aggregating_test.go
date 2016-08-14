package aggregating

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Combaine/Combaine/common/configs"
	"github.com/Combaine/Combaine/common/tasks"
	"github.com/Sirupsen/logrus"
)

const (
	cfgName  = "forAggCore"
	repoPath = "../tests/fixtures/configs"
)

func TestAggregating(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	repo, err := configs.NewFilesystemRepository(repoPath)
	assert.NoError(t, err, "Unable to create repo %s", err)
	pcfg, err := repo.GetParsingConfig(cfgName)
	assert.NoError(t, err, "unable to read parsingCfg %s: %s", cfgName, err)
	acfg, err := repo.GetAggregationConfig(cfgName)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", cfgName, err)

	var aggTask tasks.AggregationTask
	aggTask.CommonTask = tasks.CommonTask{
		Id: "testId", PrevTime: 1, CurrTime: 61,
	}
	assert.NoError(t, pcfg.Decode(&aggTask.ParsingConfig))
	assert.NoError(t, acfg.Decode(&aggTask.AggregationConfig))

	aggTask.Hosts = map[string][]string{
		"DC1": []string{"Host1", "Host2"},
		"DC2": []string{"Host3", "Host4"},
	}

	assert.NoError(t, Aggregating(&aggTask))
}
