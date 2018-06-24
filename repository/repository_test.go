package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const repopath = "../tests/testdata/configs"

func TestUtilityFunctions(t *testing.T) {
	assert.False(t, isConfig("blabla"))
	assert.False(t, isConfig("blabla.json"))
	assert.True(t, isConfig("blabla.yaml"))
}

func TestRepository(t *testing.T) {

	var (
		expectedPcfg   = []string{"aggCore", "img_status"}
		expectedAggcfg = []string{"aggCore", "badaggCore", "http_ok", "notPerHostaggCore"}
	)

	err := Init("/not_existing/dir/")
	assert.Error(t, err)
	_, err = ListAggregationConfigs()
	assert.Error(t, err)
	assert.Equal(t, GetBasePath(), "/not_existing/dir/")

	err = Init(repopath)
	assert.Nil(t, err, fmt.Sprintf("Unable to create repo %s", err))

	lp, _ := ListParsingConfigs()
	assert.Equal(t, expectedPcfg, lp, "")
	la, _ := ListAggregationConfigs()
	assert.Equal(t, expectedAggcfg, la, "")

	cmbCg := GetCombainerConfig()
	err = VerifyCombainerConfig(&cmbCg)
	assert.Nil(t, err)
	if len(cmbCg.CloudSection.HostFetcher) == 0 {
		t.Fatal("section isn't supposed to empty")
	}

	for _, name := range lp {
		pcfg, err := GetParsingConfig(name)
		assert.Nil(t, err, fmt.Sprintf("unable to read %s: %s", name, err))

		assert.NotNil(t, pcfg, "ooops")

		var decodedCfg ParsingConfig
		assert.Nil(t, pcfg.Decode(&decodedCfg), "unable to Decode parsing config")
	}

	for _, name := range la {
		pcfg, err := GetAggregationConfig(name)
		assert.Nil(t, err, fmt.Sprintf("unable to read %s: %s", name, err))
		assert.NotNil(t, pcfg, "oops")

		var decodedCfg AggregationConfig
		assert.Nil(t, pcfg.Decode(&decodedCfg), "unable to Decode aggregation config")
	}
}
