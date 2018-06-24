package repository

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	"github.com/alecthomas/template"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

const (
	// Well-known field to explore plugin name
	typeKey = "type"
	// DefaultConfigsPath default directory with combainer configs
	DefaultConfigsPath = "/etc/combaine"
	parsingSuffix      = "parsing"
	aggregateSuffix    = "aggregate"
	combaineConfig     = "combaine.yaml"
)

var (
	mainRepository *filesystemRepository
)

// MainSection describes Main section in combaine.yaml
type MainSection struct {
	ClusterConfig    ClusterConfig `yaml:"Cluster"`
	ParallelParsings int           `yaml:"ParallelParsings"`
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
	BindAddr      string `yaml:"BindAddr"`
	RaftPort      int    `yaml:"RaftPort"`
	StartAsLeader bool   `yaml:"StartAsLeader"`
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

// Init initialize config repository
func Init(basepath string) error {
	_, err := NewCombaineConfig(path.Join(basepath, combaineConfig))
	if err != nil {
		return fmt.Errorf("unable to load combaine.yaml: %s", err)
	}

	mainRepository = &filesystemRepository{
		basepath:        basepath,
		parsingpath:     path.Join(basepath, parsingSuffix),
		aggregationpath: path.Join(basepath, aggregateSuffix),
	}

	return nil
}

type filesystemRepository struct {
	basepath        string
	parsingpath     string
	aggregationpath string
}

// GetBasePath return basepath of the repository
func GetBasePath() string {
	return mainRepository.basepath
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

// GetAggregationConfigs return aggregation configs for specified parsing config
func GetAggregationConfigs(pConfig *ParsingConfig, pName string) (*map[string]AggregationConfig, error) {
	aggregationConfigs := make(map[string]AggregationConfig)
	if len(pConfig.AggConfigs) == 0 {
		pConfig.AggConfigs = []string{pName}
	}
	for _, name := range pConfig.AggConfigs {
		content, err := GetAggregationConfig(name)
		if err != nil {
			// It seems better to throw error here instead of
			// going data processing on without config
			logrus.Errorf("Unable to read aggregation config %s, %s", name, err)
			return nil, err
		}

		if len(pConfig.Placeholders) > 0 {
			content, err = content.Generate(&pConfig.Placeholders)
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

// GetAggregationConfig load aggregation config
func GetAggregationConfig(name string) (EncodedConfig, error) {
	fpath := path.Join(mainRepository.aggregationpath, name+".yaml")
	return readConfig(fpath)
}

// GetParsingConfig load parsing config
func GetParsingConfig(name string) (EncodedConfig, error) {
	fpath := path.Join(mainRepository.parsingpath, name+".yaml")
	return readConfig(fpath)
}

// GetCombainerConfig load conbainer's main config
func GetCombainerConfig() (cfg CombainerConfig) {
	cfg, _ = NewCombaineConfig(path.Join(mainRepository.basepath, combaineConfig))
	return cfg
}

// ListParsingConfigs list all parsing configs in the repository
func ListParsingConfigs() ([]string, error) {
	return lsConfigs(mainRepository.parsingpath)
}

// ListAggregationConfigs list all aggregation configs in the repository
func ListAggregationConfigs() ([]string, error) {
	return lsConfigs(mainRepository.aggregationpath)
}

func lsConfigs(filepath string) (list []string, err error) {
	listing, err := ioutil.ReadDir(filepath)
	if err != nil {
		return
	}

	for _, file := range listing {
		name := file.Name()
		if isConfig(name) && !file.IsDir() {
			list = append(list, strings.TrimSuffix(name, path.Ext(name)))
		}
	}
	return
}

func isConfig(name string) bool {
	if strings.HasSuffix(name, ".yaml") {
		return true
	}
	return false
}

func readConfig(path string) (data EncodedConfig, err error) {
	data, err = ioutil.ReadFile(path)
	if err != nil {
		return
	}
	return
}

// VerifyCombainerConfig check combainer config
func VerifyCombainerConfig(cfg *CombainerConfig) error {
	if cfg.MainSection.IterationDuration <= 0 {
		return errors.New("MINIMUM_PERIOD must be positive")
	}
	if cfg.MainSection.Cache.TTL <= 0 {
		cfg.MainSection.Cache.TTL = 5
	}
	if cfg.MainSection.Cache.Interval <= 0 {
		cfg.MainSection.Cache.Interval = 15
	}
	return nil
}

// AggregationConfig represent serction aggregation
// from combainer cilent config
type AggregationConfig struct {
	// Configuration of possible senders
	Senders map[string]PluginConfig `yaml:"senders"`
	// Configuration of data habdlers
	Data map[string]PluginConfig `yaml:"data"`
}

// Encode encode aggregation config
func (a *AggregationConfig) Encode() ([]byte, error) {
	return yaml.Marshal(a)
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

// Encode encode parsing config
func (p *ParsingConfig) Encode() ([]byte, error) {
	return yaml.Marshal(p)
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

// EncodedConfig is the bytes of the configs readed
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
