package aggregating

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Sirupsen/logrus"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/tasks"
)

const (
	cfgName  = "forAggCore"
	repoPath = "../tests/fixtures/configs"
)

func TestAggregating(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	repo, err := configs.NewFilesystemRepository(repoPath)
	if !assert.Nil(t, err, "Unable to create repo %s", err) {
		t.Fatal()
	}
	pcfg, err := repo.GetParsingConfig(cfgName)
	if !assert.Nil(t, err, "unable to read parsingCfg %s: %s", cfgName, err) {
		t.Fatal()
	}
	acfg, err := repo.GetAggregationConfig(cfgName)
	if !assert.Nil(t, err, "unable to read aggCfg %s: %s", cfgName, err) {
		t.Fatal()
	}

	var aggTask tasks.AggregationTask
	aggTask.CommonTask = tasks.CommonTask{
		Id: "testId", PrevTime: 1, CurrTime: 61,
	}
	assert.Nil(t, pcfg.Decode(&aggTask.ParsingConfig),
		"unable to Decode parsing config")
	assert.Nil(t, acfg.Decode(&aggTask.AggregationConfig),
		"unable to Decode aggregating config")

	aggTask.Hosts = map[string][]string{
		"DC1": []string{"Host1", "Host2"},
		"DC2": []string{"Host3", "Host4"},
	}
	Aggregating(aggTask)
}
