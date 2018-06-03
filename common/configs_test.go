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
	aggCfgs, err := GetAggregationConfigs(repo, &parsingConfig, "aggCore")
	assert.NotNil(t, (*aggCfgs)["aggCore"])
	assert.Len(t, *aggCfgs, 1)
	assert.NoError(t, err)
}

func TestConfigs(t *testing.T) {
	assert.Equal(t, VerifyCombainerConfig(&CombainerConfig{}).Error(), "MINIMUM_PERIOD must be positive")

	cfg := make(PluginConfig)
	_, err := cfg.Type()
	assert.Error(t, err)
	cfg["type"] = 10
	_, err = cfg.Type()
	assert.Error(t, err)
	cfg["type"] = "type"
	typ, err := cfg.Type()
	assert.Nil(t, err)
	assert.Equal(t, "type", typ)

	a := PluginConfig{"key": "here"}
	b := make(PluginConfig)
	PluginConfigsUpdate(&b, &a)
	assert.Equal(t, a, b)

	cmbCfg := &CombainerConfig{
		CombainerSection: CombainerSection{
			MainSection: MainSection{
				IterationDuration: 10,
			},
		},
		CloudSection: CloudSection{
			DataFetcher: PluginConfig{
				"combainerData": "fetcherCombainer",
				"shared":        "CombainerValueData",
			},
			HostFetcher: PluginConfig{
				"combainerHost": "fetcherCombainer",
				"shared":        "CombainerValueHost",
			},
		},
	}

	pCfg := &ParsingConfig{
		Groups: []string{"metahost"},
		DataFetcher: PluginConfig{
			"parsingData": "fetcherParsing",
			"shared":      "ParsingValueData",
		},
		HostFetcher: PluginConfig{
			"parsingHost": "fetcherParsing",
			"shared":      "ParsingValueHost",
		},
	}

	pCfg.UpdateByCombainerConfig(cmbCfg)
	assert.EqualValues(t, cmbCfg.DataFetcher, pCfg.DataFetcher)
	assert.EqualValues(t, cmbCfg.HostFetcher, pCfg.HostFetcher)
	assert.True(t, pCfg.Metahost == pCfg.Groups[0])
	assert.True(t, pCfg.IterationDuration == cmbCfg.IterationDuration)
}
