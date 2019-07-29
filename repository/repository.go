package repository

import (
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

const (
	// Well-known field to explore plugin name
	typeKey  = "type"
	classKey = "class"
	// DefaultConfigsPath default directory with combainer configs
	DefaultConfigsPath = "/etc/combaine"
	parsingSuffix      = "parsing"
	aggregateSuffix    = "aggregate"
	combaineConfig     = "combaine.yaml"
)

var (
	mainRepository *filesystemRepository
)

type filesystemRepository struct {
	basepath        string
	parsingpath     string
	aggregationpath string
}

// Init initialize config repository
func Init(basepath string) error {
	_, err := NewCombaineConfig(path.Join(basepath, combaineConfig))
	mainRepository = &filesystemRepository{
		basepath:        basepath,
		parsingpath:     path.Join(basepath, parsingSuffix),
		aggregationpath: path.Join(basepath, aggregateSuffix),
	}
	return err
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

// Encode encode aggregation config
func (a *AggregationConfig) Encode() ([]byte, error) {
	return yaml.Marshal(a)
}

// UpdateByCombainerConfig merge server config with parsing config
func (p *ParsingConfig) UpdateByCombainerConfig(config *CombainerConfig) {
	if p.IterationDuration == 0 {
		p.IterationDuration = config.MainSection.IterationDuration
	}
	if p.DistributeAggregation == "" {
		p.DistributeAggregation = config.MainSection.DistributeAggregation
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

// Class check plugin config class
func (p *PluginConfig) Class() (className string, err error) {
	rawClassName, ok := (*p)[classKey]
	if !ok {
		err = fmt.Errorf("Missing `class` value")
		return
	}

	switch t := rawClassName.(type) {
	case string, []byte:
		className = fmt.Sprintf("%s", rawClassName)
	default:
		err = fmt.Errorf("Invalid `class` argument type. String is expected. Got %s", t)
	}

	return
}

// GetBool value if present or false
func (p *PluginConfig) GetBool(key string) (bool, error) {
	var val bool
	if rawVal, ok := (*p)[key]; ok {
		val, ok = rawVal.(bool)
		if !ok {
			return false, errors.Errorf("%s is not bool value", key)
		}
	}
	return val, nil
}

// PluginConfigsUpdate update target PluginConfig with
// content from source PluginConfig
func PluginConfigsUpdate(target *PluginConfig, source *PluginConfig) {
	for k, v := range *source {
		(*target)[k] = v
	}
}

// Decode Unmarshal yaml config
func (e *EncodedConfig) Decode(inplace interface{}) error {
	return yaml.Unmarshal(*e, inplace)
}
