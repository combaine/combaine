package tests

import "github.com/combaine/combaine/common/configs"

// DummyRepo for repository
type DummyRepo []string

// ListParsingConfigs list configs from dummy repo
func (d *DummyRepo) ListParsingConfigs() ([]string, error) { return []string(*d), nil }

// ListAggregationConfigs return empty list
func (d *DummyRepo) ListAggregationConfigs() (l []string, e error) { return l, e }

// GetAggregationConfig return empty config for any name
func (d *DummyRepo) GetAggregationConfig(name string) (c configs.EncodedConfig, e error) {
	return c, e
}

// GetParsingConfig return empty config for any name
func (d *DummyRepo) GetParsingConfig(name string) (c configs.EncodedConfig, e error) {
	return c, e
}

// GetCombainerConfig return empty config
func (d *DummyRepo) GetCombainerConfig() (c configs.CombainerConfig) { return c }

// ParsingConfigIsExists always true
func (d *DummyRepo) ParsingConfigIsExists(name string) bool { return true }

// NewRepo create DummyRepo from list of configs
func NewRepo(repo []string) *DummyRepo {
	return (*DummyRepo)(&repo)
}
