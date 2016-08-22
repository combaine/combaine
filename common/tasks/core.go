package tasks

import "github.com/combaine/combaine/common/configs"

type CommonTask struct {
	Id       string `codec:"Id"`
	PrevTime int64  `codec:"PrevTime"`
	CurrTime int64  `codec:"CurrTime"`
}

type ParsingResult map[string]interface{}

type AggregationResult map[string]ParsingResult

type SenderPayload struct {
	CommonTask
	Config configs.PluginConfig
	Data   AggregationResult
}
