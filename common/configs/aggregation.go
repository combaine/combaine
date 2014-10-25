package configs

type AggregationConfig struct {
	// Configuration of possible senders
	Senders map[string]PluginConfig `yaml:"senders"`
	// Configuration of data habdlers
	Data map[string]PluginConfig `yaml:"data"`
}
