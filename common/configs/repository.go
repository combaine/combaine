package configs

import (
	"fmt"
	"io/ioutil"
	"path"
	"strings"
)

const (
	parsing_suffix    = "parsing"
	aggregatio_suffix = "aggregate"
	combaine_config   = "combaine.yaml"
)

var (
	config_suffixes = []string{".yaml", ".json"}
)

type Repository interface {
	GetAggregationConfig(name string) (EncodedConfig, error)
	GetParsingConfig(name string) (EncodedConfig, error)
	GetCombainerConfig() CombainerConfig
	ListParsingConfigs() ([]string, error)
	ListAggregationConfigs() ([]string, error)
}

func NewFilesystemRepository(basepath string) (Repository, error) {
	_, err := NewCombaineConfig(path.Join(basepath, combaine_config))
	if err != nil {
		return nil, fmt.Errorf("unable to load combaine.yaml: %s", err)
	}

	f := &filesystemRepository{
		basepath:        basepath,
		parsingpath:     path.Join(basepath, parsing_suffix),
		aggregationpath: path.Join(basepath, aggregatio_suffix),
	}

	return f, nil
}

type filesystemRepository struct {
	basepath        string
	parsingpath     string
	aggregationpath string
}

func (f *filesystemRepository) GetAggregationConfig(name string) (config EncodedConfig, err error) {
	for _, suffix := range config_suffixes {
		fpath := path.Join(f.aggregationpath, name+suffix)
		if config, err = NewAggregationConfig(fpath); err != nil {
			continue
		}
		return
	}
	return
}

func (f *filesystemRepository) GetParsingConfig(name string) (config EncodedConfig, err error) {
	for _, suffix := range config_suffixes {
		fpath := path.Join(f.parsingpath, name+suffix)
		if config, err = NewParsingConfig(fpath); err != nil {
			continue
		}
		return
	}
	return
}

func (f *filesystemRepository) GetCombainerConfig() (cfg CombainerConfig) {
	cfg, _ = NewCombaineConfig(path.Join(f.basepath, combaine_config))
	return cfg
}

func (f *filesystemRepository) ListParsingConfigs() ([]string, error) {
	return lsConfigs(f.parsingpath)
}

func (f *filesystemRepository) ListAggregationConfigs() ([]string, error) {
	return lsConfigs(f.aggregationpath)
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
	for _, suffix := range config_suffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
