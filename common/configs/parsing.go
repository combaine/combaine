package configs

type ParsingConfig struct {
	// List of host groups
	// MUST consist of 1 value now.
	Groups []string `yaml:"groups"`
	// List of names of Aggregation configs
	AggConfigs []string `yaml:"agg_configs"`
	// Name of parsing function, which is used to parse data
	// Set it `NullParser` or leave empty
	// to skip the parsing of data.
	Parser string `yaml:"parser"`
	// Overrides the same section in combainer.yaml
	DataFetcher PluginConfig `yaml:"DataFetcher"`
	// Overrides name of host group
	Metahost string `yaml:"metahost"`
	// Set True to skip putting data into DataBase
	Raw bool `yaml:"raw"`
}
