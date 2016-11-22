package configs

const (
	//ParserSkipValue is a special parser name that allows to avoid a parser
	ParserSkipValue = "NullParser"
)

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
	DataFetcher PluginConfig `yaml:"DataFetcher,omitempty"`
	// Overrides name of host group
	Metahost string `yaml:"metahost",codec:"metahost"`
	// Set True to skip putting data into DataBase
	Raw         bool `yaml:"raw"`
	MainSection `yaml:"Combainer"`
	// Overrides the same section in combainer.yaml
	HostFetcher PluginConfig `yaml:"HostFetcher,omitempty"`
	// Placeholders for template
	Placeholders map[string]interface{} `yaml:"Placeholders,omitempty"`
}

func (p *ParsingConfig) GetMetahost() string {
	if p.Metahost == "" {
		p.Metahost = p.Groups[0]
	}

	return p.Metahost
}

func (p *ParsingConfig) SkipParsingStage() bool {
	return p.Parser == ParserSkipValue || p.Parser == ""
}

func (p *ParsingConfig) UpdateByCombainerConfig(config *CombainerConfig) error {
	if p.IterationDuration == 0 {
		p.IterationDuration = config.MainSection.IterationDuration
	}

	PluginConfigsUpdate(&config.CloudSection.DataFetcher, &p.DataFetcher)
	p.DataFetcher = config.CloudSection.DataFetcher
	PluginConfigsUpdate(&config.CloudSection.HostFetcher, &p.HostFetcher)
	p.HostFetcher = config.CloudSection.HostFetcher

	return nil
}
