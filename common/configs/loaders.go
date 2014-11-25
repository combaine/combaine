package configs

import (
	"io/ioutil"
	"launchpad.net/goyaml"
)

type EncodedConfig []byte

func (e *EncodedConfig) Decode(inplace interface{}) error {
	return goyaml.Unmarshal(*e, inplace)
}

func NewParsingConfig(path string) (EncodedConfig, error) {
	return newConfig(path)
}

func NewAggregationConfig(path string) (EncodedConfig, error) {
	return newConfig(path)
}

func NewCombaineConfig(path string) (config CombainerConfig, err error) {
	data, err := newConfig(path)
	if err != nil {
		return
	}

	err = goyaml.Unmarshal(data, &config)
	return
}

func newConfig(path string) (data EncodedConfig, err error) {
	data, err = ioutil.ReadFile(path)
	if err != nil {
		return
	}

	return
}
