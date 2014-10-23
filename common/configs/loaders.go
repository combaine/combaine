package configs

import (
	"io/ioutil"
	"launchpad.net/goyaml"
)

func NewParsingConfig(path string) (config ParsingConfig, err error) {
	err = newConfig(path, &config)
	return
}

func NewAggregationConfig(path string) (config AggregationConfig, err error) {
	err = newConfig(path, &config)
	return
}

func NewCombaineConfig(path string) (config CombainerConfig, err error) {
	err = newConfig(path, &config)
	return
}

func newConfig(path string, inplace interface{}) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = goyaml.Unmarshal(data, inplace)
	if err != nil {
		return err
	}
	return nil
}
