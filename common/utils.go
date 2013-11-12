package common

import (
	"errors"
	"github.com/ugorji/go/codec"
	"launchpad.net/goyaml"
)

var (
	mh codec.MsgpackHandle
	h  = &mh
)

//Utils
func Encode(data []byte, res interface{}) (err error) {
	err = goyaml.Unmarshal(data, res)
	return
}

func Pack(input interface{}) (buf []byte, err error) {
	err = codec.NewEncoderBytes(&buf, h).Encode(input)
	return
}

func Unpack(data []byte, res interface{}) (err error) {
	err = codec.NewDecoderBytes(data, h).Decode(res)
	return
}

func GetType(cfg map[string]interface{}) (string, error) {
	if value, ok := cfg["type"]; ok {
		if Type, ok := value.(string); ok {
			return Type, nil
		} else {
			return "", errors.New("type field isn't string")
		}
	} else {
		return "", errors.New("Missing field type")
	}
}

func MapUpdate(source *map[string]interface{}, update *map[string]interface{}) {
	for k, v := range *update {
		(*source)[k] = v
	}
}
