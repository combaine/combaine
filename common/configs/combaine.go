package configs

import "errors"

// MainSection describes Main section in combaine.yaml
type MainSection struct {
	ClusterConfig    ClusterConfig `yaml:"Cluster"`
	ParallelParsings int           `yaml:"ParallelParsings"`
	// Duration of iteration in sec
	// Pasring stage longs at least 0.8 * MinimumPeriod
	IterationDuration uint `yaml:"MINIMUM_PERIOD"`
	// Group of cloud machines
	CloudGroup string `yaml:"cloud"`
	// Cache options
	Cache PluginConfig `yaml:"Cache,omitempty"`
}

// ClusterConfig about serf and raft
type ClusterConfig struct {
	SnapshotPath string `yaml:"SnapshotPath"`
	BindAddr     string `yaml:"BindAddr"`
}

// LockServerSection zk related section
type LockServerSection struct {
	ID string `yaml:"app_id"`
	// Array of Zookeeper hosts
	Hosts []string `yaml:"host"`
	Name  string   `yaml:"name"`
	// Connection timeout
	Timeout uint `yaml:"timeout"`
}

// CloudSection configure fetchers and discovery
type CloudSection struct {
	// Default DataFetcher
	DataFetcher PluginConfig `yaml:"DataFetcher"`
	// Hosts for AgavePlugin
	AgaveHosts  []string     `yaml:"agave_hosts"`
	HostFetcher PluginConfig `yaml:"HostFetcher"`
}

// CombainerSection about combainer daemon configs
type CombainerSection struct {
	LockServerSection `yaml:"Lockserver"`
	MainSection       `yaml:"Main"`
}

// CombainerConfig container for all other configs
type CombainerConfig struct {
	CombainerSection `yaml:"Combainer"`
	CloudSection     `yaml:"cloud_config"`
}

// VerifyCombainerConfig check combainer config
func VerifyCombainerConfig(cfg *CombainerConfig) error {
	if cfg.MainSection.IterationDuration <= 0 {
		return errors.New("MINIMUM_PERIOD must be positive")
	}
	return nil
}
