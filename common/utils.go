package common

import (
	"errors"
	"fmt"

	"github.com/ugorji/go/codec"
	"gopkg.in/yaml.v2"
)

var (
	mh codec.MsgpackHandle
	h  = &mh
)

//Utils
func Decode(data []byte, res interface{}) error {
	return yaml.Unmarshal(data, res)
}

func Encode(in interface{}) ([]byte, error) {
	return yaml.Marshal(in)
}

func Pack(input interface{}) (buf []byte, err error) {
	err = codec.NewEncoderBytes(&buf, h).Encode(input)
	return
}

func Unpack(data []byte, res interface{}) error {
	return codec.NewDecoderBytes(data, h).Decode(res)
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

func InterfaceToString(v interface{}) (s string) {
	switch v := v.(type) {
	case int:
		s = fmt.Sprintf("%d", v)
	case float32, float64:
		s = fmt.Sprintf("%f", v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return
}
