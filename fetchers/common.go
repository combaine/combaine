package fetchers

import (
	"github.com/combaine/combaine/common"
	"github.com/mitchellh/mapstructure"
)

const defaultTimeout = 5000 // time.Millisecond

func decodeConfig(cfg common.PluginConfig, result interface{}) error {
	decoderConfig := mapstructure.DecoderConfig{
		WeaklyTypedInput: true, // To allow decoder parses []uint8 as string
		Result:           result,
	}
	decoder, err := mapstructure.NewDecoder(&decoderConfig)
	if err != nil {
		return err
	}
	return decoder.Decode(cfg)
}
