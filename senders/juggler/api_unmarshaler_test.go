package juggler

import (
	"testing"

	"github.com/combaine/combaine/common"
	"github.com/stretchr/testify/assert"
)

const repoPath = "../../tests/testdata/configs"
const cfgName = "aggCore"

var (
	repo              common.Repository
	acfg              common.EncodedConfig
	aggregationConfig common.AggregationConfig
)

func thisTestInit(t *testing.T) {
	var err error
	repo, err = common.NewFilesystemRepository(repoPath)
	assert.NoError(t, err, "Unable to create repo %s", err)
	acfg, err = repo.GetAggregationConfig(cfgName)
	assert.NoError(t, err, "unable to read aggCfg %s: %s", cfgName, err)
	assert.NoError(t, acfg.Decode(&aggregationConfig))
}

func TestCustomUnmarshaler(t *testing.T) {
	thisTestInit(t)
	for _, senderConfig := range aggregationConfig.Senders {
		sType, err := senderConfig.Type()
		assert.NoError(t, err, "Failed to check sender type")
		if sType != "juggler" {
			continue
		}
		encodedSenderConfig, err := common.Pack(senderConfig)
		assert.NoError(t, err, "Failed to pack aggregation config")
		//logger.Debugf("%#v", senderConfig)
		var nowDecodedConfig Config
		err = common.Unpack(encodedSenderConfig, &nowDecodedConfig)
		if err != nil {
			panic(err)
		}
		//logger.Debugf("%#v", nowDecodedConfig)
		assert.NoError(t, err, "Failed to decode juggler config")
	}
}
