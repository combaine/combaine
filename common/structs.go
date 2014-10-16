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
	DG         map[string]interface{} `yaml:"LocalDatabase"`
	Metahost   string                 `yaml:"metahost"`
	Raw        bool                   `yaml:"raw"`
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
		DF    map[string]interface{} `yaml:"DataFetcher"`
		DG    map[string]interface{} `yaml:"LocalDatabase"`
		Agave []string               `yaml:"agave_hosts"`
	} `yaml:"cloud_config"`
}

type FetcherTask struct {
	Id        string "Id"
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
	Metahost string
}

func (t *ParsingTask) String() string {
	return fmt.Sprintf("%v", t)
}

// Aggregate task
type AggregationTask struct {
	Config   string
	PConfig  string
	Group    string
	PrevTime int64
	CurrTime int64
	Id       string
	Metahost string
}

// For senders
type DataItem map[string]interface{}
type DataType map[string]DataItem
