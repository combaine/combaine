package common

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

const (
	parsingSuffix   = "parsing"
	aggregateSuffix = "aggregate"
	combaineConfig  = "combaine.yaml"
)

var (
	configSuffixes = []string{".yaml", ".json"}
)

// Repository interface to access to combainer's parsing and aggregations configs
type Repository interface {
	GetAggregationConfig(name string) (EncodedConfig, error)
	GetParsingConfig(name string) (EncodedConfig, error)
	GetCombainerConfig() CombainerConfig
	ParsingConfigIsExists(name string) bool
	ListParsingConfigs() ([]string, error)
	ListAggregationConfigs() ([]string, error)
}

// NewFilesystemRepository implement Repository interface
func NewFilesystemRepository(basepath string) (Repository, error) {
	_, err := NewCombaineConfig(path.Join(basepath, combaineConfig))
	if err != nil {
		return nil, fmt.Errorf("unable to load combaine.yaml: %s", err)
	}

	f := &filesystemRepository{
		basepath:        basepath,
		parsingpath:     path.Join(basepath, parsingSuffix),
		aggregationpath: path.Join(basepath, aggregateSuffix),
	}

	return f, nil
}

type filesystemRepository struct {
	basepath        string
	parsingpath     string
	aggregationpath string
}

func (f *filesystemRepository) GetAggregationConfig(name string) (config EncodedConfig, err error) {
	for _, suffix := range configSuffixes {
		fpath := path.Join(f.aggregationpath, name+suffix)
		if config, err = NewAggregationConfig(fpath); err != nil {
			continue
		}
		return
	}
	return
}

func (f *filesystemRepository) GetParsingConfig(name string) (config EncodedConfig, err error) {
	for _, suffix := range configSuffixes {
		fpath := path.Join(f.parsingpath, name+suffix)
		if config, err = NewParsingConfig(fpath); err != nil {
			continue
		}
		return
	}
	return
}

func (f *filesystemRepository) GetCombainerConfig() (cfg CombainerConfig) {
	cfg, _ = NewCombaineConfig(path.Join(f.basepath, combaineConfig))
	return cfg
}

func (f *filesystemRepository) ListParsingConfigs() ([]string, error) {
	return lsConfigs(f.parsingpath)
}

func (f *filesystemRepository) ListAggregationConfigs() ([]string, error) {
	return lsConfigs(f.aggregationpath)
}

func (f *filesystemRepository) ParsingConfigIsExists(name string) bool {
	for _, suffix := range configSuffixes {
		fpath := path.Join(f.parsingpath, name+suffix)
		if _, err := os.Stat(fpath); err == nil {
			return true
		}
	}
	return false
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
	for _, suffix := range configSuffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// GRPCTracingIsEnabled return tracing parameter value from combainer configs
func GRPCTracingIsEnabled(configdir string) bool {
	cfg, err := NewCombaineConfig(path.Join(configdir, combaineConfig))
	if err != nil {
		return false
	}
	return cfg.EnableGRPCTracing
}
