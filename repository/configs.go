package repository

// MainSection describes Main section in combaine.yaml
type MainSection struct {
	ClusterConfig    ClusterConfig `yaml:"Cluster"`
	ParallelParsings int           `yaml:"ParallelParsings"`
	// distribute aggregations across a "cluster", or make them "local"ly
	DistributeAggregation string `yaml:"DistributeAggregation"`
	// Duration of iteration in sec
	// Pasring stage longs at least 0.8 * MinimumPeriod
	IterationDuration uint `yaml:"MINIMUM_PERIOD"`
	// Groups of cloud machines
	CloudGroups []string `yaml:"cloud"`
	// combaine cloud hosts fetcher
	HostFetcher PluginConfig `yaml:"HostFetcher,omitempty"`
	// Cache TTLCache options
	Cache CacheConfig `yaml:"cache,omitempty"`
}

// CacheConfig for TTLCache
type CacheConfig struct {
	TTL      int64 `yaml:"ttl"`
	Interval int64 `yaml:"interval"`
}

// ClusterConfig about serf and raft
type ClusterConfig struct {
	BindAddr string `yaml:"BindAddr"`
	RaftPort int    `yaml:"RaftPort"`
	// expect serf nodes to bootstrap raft cluster
	BootstrapExpect uint `yaml:"BootstrapExpect"`
	StartAsLeader   bool `yaml:"StartAsLeader"`
}

// CloudSection configure fetchers and discovery
type CloudSection struct {
	// Default DataFetcher
	DataFetcher PluginConfig `yaml:"DataFetcher"`
	HostFetcher PluginConfig `yaml:"HostFetcher"`
}

// CombainerSection about combainer daemon configs
type CombainerSection struct {
	MainSection `yaml:"Main"`
}

// CombainerConfig container for all other configs
type CombainerConfig struct {
	CombainerSection `yaml:"Combainer"`
	CloudSection     `yaml:"cloud_config"`
}

// AggregationConfig represent serction aggregation
// from combainer cilent config
type AggregationConfig struct {
	// Configuration of possible senders
	Senders map[string]PluginConfig `yaml:"senders"`
	// Configuration of data habdlers
	Data map[string]PluginConfig `yaml:"data"`
}

// ParsingConfig contains settings from parsing section of combainer configs
type ParsingConfig struct {
	// List of host groups
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

// PluginConfig general description
// of any user-defined plugin configuration section
type PluginConfig map[string]interface{}

// EncodedConfig is the bytes of the configs readed
type EncodedConfig []byte
