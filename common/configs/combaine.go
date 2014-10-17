package configs

// Describes Main section in combaine.yaml
type MainSection struct {
	// HTTP template. It's substituted with group name
	// to get hosts in groups.
	Http_hand string `yaml:"HTTP_HAND"`
	// Duration of iteration in sec
	// Pasring stage longs at least 0.8 * MinimumPeriod
	IterationDuration uint "MINIMUM_PERIOD"
	// Group of cloud machines
	CloudGroup string `yaml:"cloud"`
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
	AgaveHosts []string `yaml:"agave_hosts"`
}

type CombainerSection struct {
	LockServerSection `yaml:"Lockserver"`
	MainSection       `yaml:"Main"`
}

type CombainerConfig struct {
	CombainerSection `yaml:"Combainer"`
	CloudSection     `yaml:"cloud_config"`
}
