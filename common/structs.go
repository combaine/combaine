package common

import (
	"fmt"
)

// Description of aggregation config
type AggConfig struct {
	Data    map[string]map[string]interface{} `yaml:"data"`
	Senders map[string]map[string]interface{} `yaml:senders`
}

//Description of parsing config
type ParsingConfig struct {
	Groups     []string               `yaml:"groups"`
	AggConfigs []string               `yaml:"agg_configs"`
	Parser     string                 `yaml:"parser"`
	DF         map[string]interface{} `yaml:"DataFetcher"`
	DS         map[string]interface{} `yaml:"DistributedStorage"`
	DG         map[string]interface{} `yaml:"LocalDatabase"`
}

// Description of combainer config
type combainerMainCfg struct {
	Http_hand       string "HTTP_HAND"
	MaxPeriod       uint   "MAXIMUM_PERIOD"
	MaxAttemps      uint   "MAX_ATTEMPS"
	MaxRespWaitTime uint   "MAX_RESP_WAIT_TIME"
	MinimumPeriod   uint   "MINIMUM_PERIOD"
	CloudHosts      string "cloud"
}

type combainerLockserverCfg struct {
	Id      string   "app_id"
	Hosts   []string "host"
	Name    string   "name"
	timeout uint     "timeout"
}

type CombainerConfig struct {
	Combainer struct {
		Main          combainerMainCfg       "Main"
		LockServerCfg combainerLockserverCfg "Lockserver"
	} "Combainer"
	CloudCfg struct {
		DF map[string]interface{} `yaml:"DataFetcher"`
		DS map[string]interface{} `yaml:"DistributedStorage"`
		DG map[string]interface{} `yaml:"LocalDatabase"`
	} `yaml:"cloud_config"`
}

type FetcherTask struct {
	Target    string "Target"
	StartTime int64  "StartTime"
	EndTime   int64  "EndTime"
}

// Parsing task
type ParsingTask struct {
	Host     string
	Config   string
	Group    string
	PrevTime int64
	CurrTime int64
	Id       string
}

func (t *ParsingTask) String() string {
	return fmt.Sprintf("%v", t)
}

// Aggregate task
type AggregationTask struct {
	Config   string
	Group    string
	PrevTime int64
	CurrTime int64
	Id       string
}
