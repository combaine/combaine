package common

import (
	"bytes"
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"

	"github.com/Sirupsen/logrus"

	yaml "gopkg.in/yaml.v2"
)

const (
	// Well-known field to explore plugin name
	typeKey = "type"
	// DefaultConfigsPath default directory with combainer configs
	DefaultConfigsPath = "/etc/combaine"
)

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
	SnapshotPath  string `yaml:"SnapshotPath"`
	DataDir       string `yaml:"DataDir"`
	BindAddr      string `yaml:"BindAddr"`
	RaftPort      int    `yaml:"RaftPort"`
	RaftStateDir  string `yaml:"RaftStateDir"`
	StartAsLeader bool   `yaml:"StartAsLeader"`
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
	MainSection       `yaml:"Main"`
	EnableGRPCTracing bool `yaml:"EnableGRPCTracing"`
}

// NewCombaineConfig load conbainer's main config
func NewCombaineConfig(path string) (config CombainerConfig, err error) {
	data, err := readConfig(path)
	if err != nil {
		return
	}

	err = yaml.Unmarshal(data, &config)
	return
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

// NewAggregationConfig load aggregation config from disk
func NewAggregationConfig(path string) (EncodedConfig, error) {
	return readConfig(path)
}

// AggregationConfig represent serction aggregation
// from combainer cilent config
type AggregationConfig struct {
	// Configuration of possible senders
	Senders map[string]PluginConfig `yaml:"senders"`
	// Configuration of data habdlers
	Data map[string]PluginConfig `yaml:"data"`
}

// GetAggregationConfigs return aggregation config for specified parsing config
func GetAggregationConfigs(repo Repository, parsingConfig *ParsingConfig) (*map[string]AggregationConfig, error) {
	aggregationConfigs := make(map[string]AggregationConfig)
	for _, name := range parsingConfig.AggConfigs {
		content, err := repo.GetAggregationConfig(name)
		if err != nil {
			// It seems better to throw error here instead of
			// going data processing on without config
			logrus.Errorf("Unable to read aggregation config %s, %s", name, err)
			return nil, err
		}

		if len(parsingConfig.Placeholders) > 0 {
			content, err = content.Generate(&parsingConfig.Placeholders)
			if err != nil {
				return nil, err
			}
		}

		var aggConfig AggregationConfig
		if err := content.Decode(&aggConfig); err != nil {
			logrus.Errorf("Unable to decode aggConfig: %s", err)
			return nil, err
		}
		aggregationConfigs[name] = aggConfig
	}

	return &aggregationConfigs, nil
}

// NewParsingConfig load parsing config from disk
func NewParsingConfig(path string) (EncodedConfig, error) {
	return readConfig(path)
}

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

// PluginConfig general description
// of any user-defined plugin configuration section
type PluginConfig map[string]interface{}

// Type check plugin config type
func (p *PluginConfig) Type() (typeName string, err error) {
	rawTypeName, ok := (*p)[typeKey]
	if !ok {
		err = fmt.Errorf("Missing `type` value")
		return
	}

	switch t := rawTypeName.(type) {
	case string, []byte:
		typeName = fmt.Sprintf("%s", rawTypeName)
	default:
		err = fmt.Errorf("Invalid `type` argument type. String is expected. Got %s", t)
	}

	return
}

// PluginConfigsUpdate update target PluginConfig with
// content from source PluginConfig
func PluginConfigsUpdate(target *PluginConfig, source *PluginConfig) {
	for k, v := range *source {
		(*target)[k] = v
	}
}

// EncodedConfig is the bytes of the configs readed from disk
type EncodedConfig []byte

// Decode Unmarshal yaml config
func (e *EncodedConfig) Decode(inplace interface{}) error {
	return yaml.Unmarshal(*e, inplace)
}

// Generate build new EncodedConfig
func (e *EncodedConfig) Generate(placeholders *map[string]interface{}) (EncodedConfig, error) {
	t, err := template.New("name").Parse(string(*e))
	if err != nil {
		return nil, err
	}

	b := new(bytes.Buffer)
	if err = t.Execute(b, placeholders); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func readConfig(path string) (data EncodedConfig, err error) {
	data, err = ioutil.ReadFile(path)
	if err != nil {
		return
	}

	return
}
