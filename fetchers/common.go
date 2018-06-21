package fetchers

import (
	"github.com/combaine/combaine/repository"
	"github.com/mitchellh/mapstructure"
)

const defaultTimeout = 5000 // time.Millisecond

func decodeConfig(cfg repository.PluginConfig, result interface{}) error {
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
