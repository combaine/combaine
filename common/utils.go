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

func MapUpdate(source map[string]interface{}, target map[string]interface{}) {
	for k, v := range source {
		target[k] = v
	}
}

func InterfaceToString(v interface{}) (s string) {
	switch v := v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		s = fmt.Sprintf("%d", v)
	case float32, float64:
		s = fmt.Sprintf("%f", v)
	case []byte:
		s = fmt.Sprintf("%q", v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return
}

type NameStack []string

func (n *NameStack) Push(item string) {
	*n = append(*n, item)
}

func (n *NameStack) Pop() (item string) {
	item, *n = (*n)[len(*n)-1], (*n)[:len(*n)-1]
	return item
}
