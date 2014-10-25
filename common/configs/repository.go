package configs

import (
	"fmt"
	"io/ioutil"
	"path"
	"strings"
	"sync"
)

const (
	parsing_suffix    = "parsing"
	aggregatio_suffix = "aggregate"
	combaine_config   = "combaine.yaml"
)

type Repository interface {
	GetAggregationConfig(name string) (AggregationConfig, error)
	GetParsingConfig(name string) (ParsingConfig, error)
	GetCombainerConfig() (CombainerConfig, error)
	ListParsingConfigs() []string
	ListAggregationConfigs() []string
}

func NewFilesystemRepository(basepath string) (Repository, error) {
	f := filesystemRepository{
		basepath:         basepath,
		parsingpath:      path.Join(basepath, parsing_suffix),
		aggregationpath:  path.Join(basepath, aggregatio_suffix),
		parsingconfigs:   make(map[string]ParsingConfig),
		aggregateconfigs: make(map[string]AggregationConfig),
	}

	err := f.initialize()

	return &f, err
}

type filesystemRepository struct {
	sync.Mutex
	basepath         string
	parsingpath      string
	aggregationpath  string
	parsingconfigs   map[string]ParsingConfig
	aggregateconfigs map[string]AggregationConfig
	combainer        *CombainerConfig
}

func (f *filesystemRepository) genNameFromFilePath(filepath string) string {
	_, name := path.Split(filepath)
	return strings.TrimSuffix(name, path.Ext(name))
}

func (f *filesystemRepository) initialize() error {
	// load parsing configs
	files, _ := ioutil.ReadDir(f.parsingpath)
	for _, file := range files {
		if isConfig(file.Name()) {
			f.loadParsingConfig(file.Name())
		}
	}

	// load aggregation config
	files, _ = ioutil.ReadDir(f.aggregationpath)
	for _, file := range files {
		if isConfig(file.Name()) {
			f.loadAggregationConfig(file.Name())
		}
	}

	cfg, err := NewCombaineConfig(path.Join(f.basepath, combaine_config))
	f.combainer = &cfg
	return err
}

func (f *filesystemRepository) GetAggregationConfig(name string) (AggregationConfig, error) {
	f.Lock()
	defer f.Unlock()
	var cfg AggregationConfig
	cfg, ok := f.aggregateconfigs[name]
	if !ok {
		return cfg, fmt.Errorf("Aggregation config %s is missing", name)
	}
	return cfg, nil
}

func (f *filesystemRepository) GetParsingConfig(name string) (ParsingConfig, error) {
	f.Lock()
	defer f.Unlock()
	var cfg ParsingConfig
	cfg, ok := f.parsingconfigs[name]
	if !ok {
		return cfg, fmt.Errorf("Parsing config %s is missing", name)
	}
	return cfg, nil
}

func (f *filesystemRepository) GetCombainerConfig() (cfg CombainerConfig, err error) {
	f.Lock()
	defer f.Unlock()
	if f.combainer != nil {
		cfg = *f.combainer
		return
	}
	err = fmt.Errorf("Combaine config is missing")
	return
}

func (f *filesystemRepository) ListParsingConfigs() (list []string) {
	f.Lock()
	defer f.Unlock()
	for k, _ := range f.parsingconfigs {
		list = append(list, k)
	}

	return list
}

func (f *filesystemRepository) ListAggregationConfigs() (list []string) {
	f.Lock()
	defer f.Unlock()
	for k, _ := range f.aggregateconfigs {
		list = append(list, k)
	}
	return list
}

func (f *filesystemRepository) loadParsingConfig(name string) error {
	f.Lock()
	defer f.Unlock()
	config, err := NewParsingConfig(path.Join(f.parsingpath, name))
	if err != nil {
		return err
	}

	f.parsingconfigs[f.genNameFromFilePath(name)] = config
	return nil
}

func (f *filesystemRepository) loadAggregationConfig(name string) error {
	f.Lock()
	defer f.Unlock()
	config, err := NewAggregationConfig(path.Join(f.aggregationpath, name))
	if err != nil {
		return err
	}

	f.aggregateconfigs[f.genNameFromFilePath(name)] = config
	return nil
}

func isConfig(name string) bool {
	return strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".yaml")
}
