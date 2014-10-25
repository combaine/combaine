package tasks

import (
	"fmt"

	"github.com/noxiouz/Combaine/common/configs"
)

type CommonTask struct {
	Id       string `codec:"Id"`
	PrevTime int64  `codec:"PrevTime"`
	CurrTime int64  `codec:"CurrTime"`
}

var (
	EmptyCommonTask = CommonTask{
		Id:       "",
		PrevTime: -1,
		CurrTime: -1}
)

type ParsingTask struct {
	CommonTask
	// Hostname of target
	Host string
	// Name of handled parsing config
	ParsingConfigName string
	// Content of the current parsing config
	ParsingConfig configs.ParsingConfig
	// Content of aggreagtion configs
	// related to the current parsing config
	AggregationConfigs map[string]configs.AggregationConfig
}

func (t *ParsingTask) String() string {
	return fmt.Sprintf("%v", t)
}

type AggregationTask struct {
	CommonTask
	// Name of the current aggregation config
	Config string
	// Name of handled parsing config
	ParsingConfigName string
	// Content of the current parsing config
	ParsingConfig configs.ParsingConfig
	// Current aggregation config
	AggregationConfig configs.AggregationConfig
}
