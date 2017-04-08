package common

import "github.com/combaine/combaine/common/configs"

// CommonTask generic task structure
type CommonTask struct {
	Id       string `codec:"Id"`
	PrevTime int64  `codec:"PrevTime"`
	CurrTime int64  `codec:"CurrTime"`
}

// ParsingResult map host to parsing results
type ParsingResult map[string]interface{}

type AggregationResult struct {
	Tags   map[string]string `codec:"Tags" yaml:"Tags"`
	Result interface{}       `codec:"Result" yaml:"Result"`
}

type SenderPayload struct {
	CommonTask
	Config configs.PluginConfig
	Data   []AggregationResult
}

type FetcherTask struct {
	CommonTask
	Target string `codec:"Target"`
}
