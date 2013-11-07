package common

import (
	"errors"
	"launchpad.net/goyaml"
)

//Utils
func Encode(data []byte, res interface{}) (err error) {
	err = goyaml.Unmarshal(data, res)
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
