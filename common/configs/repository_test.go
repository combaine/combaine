package configs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtilityFunctions(t *testing.T) {
	assert.False(t, isConfig("blabla"))
	assert.True(t, isConfig("blabla.json"))
	assert.True(t, isConfig("blabla.yaml"))
}

func TestRepository(t *testing.T) {
	const (
		repopath = "../../tests/fixtures/configs"
	)

	var (
		expectedPcfg      = []string{"aggCore", "img_status"}
		expectedAggcfg    = []string{"aggCore", "http_ok"}
		expectedLockHosts = []string{"localhost:2181"}
	)

	repo, err := NewFilesystemRepository(repopath)
	if !assert.Nil(t, err) {
		t.Fatalf("Unable to create repo %s", err)
	}

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
		if !assert.Nil(t, err) {
			t.Fatalf("unable to read %s: %s", name, err)
		}

		if !repo.ParsingConfigIsExists(name) {
			t.Fatalf("Parsing config %s don't exists", name)
		}

		not_existing := "balbla"
		if repo.ParsingConfigIsExists(not_existing) {
			t.Fatalf("No existing config %s mistakenly identified as there", not_existing)
		}

		if !assert.NotNil(t, pcfg) {
			t.Fatal("ooops")
		}

		var decodedCfg ParsingConfig
		assert.Nil(t, pcfg.Decode(&decodedCfg), "unable to Decode parsing config")
	}

	for _, name := range la {
		pcfg, err := repo.GetAggregationConfig(name)
		if !assert.Nil(t, err) {
			t.Fatalf("unable to read %s: %s", name, err)
		}

		if !assert.NotNil(t, pcfg) {
			t.Fatal("ooops")
		}

		var decodedCfg AggregationConfig
		assert.Nil(t, pcfg.Decode(&decodedCfg), "unable to Decode aggregation config")
	}
}
