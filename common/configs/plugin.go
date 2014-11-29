package configs

import (
	"fmt"
)

const (
	// Well-known field to explore plugin name
	typeKey = "type"
)

// General description of any user-defined
// plugin configuration section
type PluginConfig map[string]interface{}

func (p *PluginConfig) Type() (type_name string, err error) {
	raw_type_name, ok := (*p)[typeKey]
	if !ok {
		err = fmt.Errorf("Missing `type` value")
		return
	}

	// if type_name, ok = raw_type_name.(string); !ok {
	// 	err = fmt.Errorf("Invalid `type` argument type. String is expected. Got %s", reflect.TypeOf(raw_type_name))
	// }

	switch t := raw_type_name.(type) {
	case string, []byte:
		type_name = fmt.Sprintf("%s", raw_type_name)
	default:
		err = fmt.Errorf("Invalid `type` argument type. String is expected. Got %s", t)
	}

	return
}

func (p *PluginConfig) HasKey(key string) bool {
	_, ok := (*p)[key]
	return ok
}

func PluginConfigsUpdate(source *PluginConfig, update *PluginConfig) {
	for k, v := range *update {
		(*source)[k] = v
	}
}
