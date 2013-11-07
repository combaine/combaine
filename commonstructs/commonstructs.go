package commonstructs

type AggConfig struct {
	Data    map[string]map[string]interface{} `yaml:"data"`
	Senders map[string]map[string]interface{} `yaml:senders`
}

type ParsingConfig struct {
	Groups     []string `yaml:"groups"`
	AggConfigs []string `yaml:"agg_configs"`
	Parser     string   `yaml:"parser"`
}
