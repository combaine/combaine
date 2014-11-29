package configs

// Describes Main section in combaine.yaml
type MainSection struct {
	// Duration of iteration in sec
	// Pasring stage longs at least 0.8 * MinimumPeriod
	IterationDuration uint "MINIMUM_PERIOD"
	// Group of cloud machines
	CloudGroup string `yaml:"cloud"`
	// Cache options
	Cache PluginConfig `yaml:"Cache,omitempty"`
}

type LockServerSection struct {
	Id string `yaml:"app_id"`
	// Array of Zookeeper hosts
	Hosts []string `yaml:"host"`
	Name  string   `yaml:"name"`
	// Connection timeout
	Timeout uint `yaml:"timeout"`
}

type CloudSection struct {
	// Default DataFetcher
	DataFetcher PluginConfig `yaml:"DataFetcher"`
	// Hosts for AgavePlugin
	AgaveHosts  []string     `yaml:"agave_hosts"`
	HostFetcher PluginConfig `yaml:"HostFetcher"`
}

type CombainerSection struct {
	LockServerSection `yaml:"Lockserver"`
	MainSection       `yaml:"Main"`
}

type CombainerConfig struct {
	CombainerSection `yaml:"Combainer"`
	CloudSection     `yaml:"cloud_config"`
}
