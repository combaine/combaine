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
	Host               string
	ParsingConfigName  string
	ParsingConfig      configs.ParsingConfig
	AggregationConfigs map[string]configs.AggregationConfig
}

func (t *ParsingTask) String() string {
	return fmt.Sprintf("%v", t)
}

type AggregationTask struct {
	CommonTask
	Config            string
	ParsingConfigName string
	ParsingConfig     configs.ParsingConfig
	AggregationConfig configs.AggregationConfig
}
