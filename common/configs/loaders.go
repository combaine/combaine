package configs

import (
	"bytes"
	"io/ioutil"
	"text/template"

	"gopkg.in/yaml.v2"
)

// EncodedConfig is the bytes of the configs readed from disk
type EncodedConfig []byte

// Decode Unmarshal yaml config
func (e *EncodedConfig) Decode(inplace interface{}) error {
	return yaml.Unmarshal(*e, inplace)
}

// Generate build new EncodedConfig
func (e *EncodedConfig) Generate(placeholders *map[string]interface{}) (EncodedConfig, error) {
	t, err := template.New("name").Parse(string(*e))
	if err != nil {
		return nil, err
	}

	b := new(bytes.Buffer)
	if err = t.Execute(b, placeholders); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// NewParsingConfig load parsing config from disk
func NewParsingConfig(path string) (EncodedConfig, error) {
	return newConfig(path)
}

// NewAggregationConfig load parsing config from disk
func NewAggregationConfig(path string) (EncodedConfig, error) {
	return newConfig(path)
}

// NewCombaineConfig load conbainer's main config
func NewCombaineConfig(path string) (config CombainerConfig, err error) {
	data, err := newConfig(path)
	if err != nil {
		return
	}

	err = yaml.Unmarshal(data, &config)
	return
}

func newConfig(path string) (data EncodedConfig, err error) {
	data, err = ioutil.ReadFile(path)
	if err != nil {
		return
	}

	return
}
