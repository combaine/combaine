package configs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtilityFunctions(t *testing.T) {
	assert.False(t, isConfig("blabla"))
	assert.True(t, isConfig("blabla.json"))
	assert.True(t, isConfig("blabla.yaml"))
}

func TestRepository(t *testing.T) {
	const repopath = "../../tests/testdata/configs"

	var (
		expectedPcfg      = []string{"aggCore", "img_status"}
		expectedAggcfg    = []string{"aggCore", "badaggCore", "http_ok", "notPerHostaggCore"}
		expectedLockHosts = []string{"localhost:2181"}
	)

	_, err := NewFilesystemRepository("/not_existing/dir/")
	assert.Error(t, err)

	repo, err := NewFilesystemRepository(repopath)
	assert.Nil(t, err, fmt.Sprintf("Unable to create repo %s", err))

	lp, _ := repo.ListParsingConfigs()
	assert.Equal(t, expectedPcfg, lp, "")
	la, _ := repo.ListAggregationConfigs()
	assert.Equal(t, expectedAggcfg, la, "")

	cmbCg := repo.GetCombainerConfig()
	assert.Equal(t, expectedLockHosts, cmbCg.LockServerSection.Hosts, "")

	if len(cmbCg.CloudSection.HostFetcher) == 0 {
		t.Fatal("section isn't supposed to empty")
	}

	for _, name := range lp {
		pcfg, err := repo.GetParsingConfig(name)
		assert.Nil(t, err, fmt.Sprintf("unable to read %s: %s", name, err))

		if !repo.ParsingConfigIsExists(name) {
			t.Fatalf("Parsing config %s don't exists", name)
		}

		notExisting := "balbla"
		if repo.ParsingConfigIsExists(notExisting) {
			t.Fatalf("No existing config %s mistakenly identified as there", notExisting)
		}

		assert.NotNil(t, pcfg, "ooops")

		var decodedCfg ParsingConfig
		assert.Nil(t, pcfg.Decode(&decodedCfg), "unable to Decode parsing config")
	}

	for _, name := range la {
		pcfg, err := repo.GetAggregationConfig(name)
		assert.Nil(t, err, fmt.Sprintf("unable to read %s: %s", name, err))
		assert.NotNil(t, pcfg, "oops")

		var decodedCfg AggregationConfig
		assert.Nil(t, pcfg.Decode(&decodedCfg), "unable to Decode aggregation config")
	}
}
