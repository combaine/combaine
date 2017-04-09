package configs

// ParsingConfig contains settings from parsing section of combainer configs
type ParsingConfig struct {
	// List of host groups
	// MUST consist of 1 value now.
	Groups []string `yaml:"groups"`
	// List of names of Aggregation configs
	AggConfigs []string `yaml:"agg_configs"`
	// Overrides the same section in combainer.yaml
	DataFetcher PluginConfig `yaml:"DataFetcher,omitempty"`
	// Overrides name of host group
	Metahost string `yaml:"metahost" codec:"metahost"`
	// MainSection contains server configs
	MainSection `yaml:"Combainer"`
	// Overrides the same section in combainer.yaml
	HostFetcher PluginConfig `yaml:"HostFetcher,omitempty"`
	// Placeholders for template
	Placeholders map[string]interface{} `yaml:"Placeholders,omitempty"`
}

// UpdateByCombainerConfig merge server config with parsing config
func (p *ParsingConfig) UpdateByCombainerConfig(config *CombainerConfig) {
	if p.IterationDuration == 0 {
		p.IterationDuration = config.MainSection.IterationDuration
	}

	PluginConfigsUpdate(&config.CloudSection.DataFetcher, &p.DataFetcher)
	p.DataFetcher = config.CloudSection.DataFetcher
	PluginConfigsUpdate(&config.CloudSection.HostFetcher, &p.HostFetcher)
	p.HostFetcher = config.CloudSection.HostFetcher

	if p.Metahost == "" && len(p.Groups) > 0 {
		p.Metahost = p.Groups[0]
	}
}
