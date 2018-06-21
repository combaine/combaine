package common

import "github.com/combaine/combaine/repository"

// Task generic task structure
type Task struct {
	Id       string `codec:"Id"`
	PrevTime int64  `codec:"PrevTime"`
	CurrTime int64  `codec:"CurrTime"`
}

// ParsingResult map host to parsing results
type ParsingResult map[string]interface{}

// AggregationResult ...
type AggregationResult struct {
	Tags   map[string]string `codec:"Tags" yaml:"Tags"`
	Result interface{}       `codec:"Result" yaml:"Result"`
}

// ParsingPayload is task for parsing stage
type ParsingPayload struct {
	Task
	Config repository.PluginConfig
	Meta   map[string]string
	Data   []byte
}

// AggregateGropuPayload is task for aggregating stage
type AggregateGropuPayload struct {
	Task
	Config repository.PluginConfig
	Meta   map[string]string
	Data   [][]byte
}

// SenderPayload is task for senders
type SenderPayload struct {
	Task
	Config repository.PluginConfig
	Data   []AggregationResult
}

// FetcherTask task for hosts fetchers
type FetcherTask struct {
	Task
	Target string `codec:"Target"`
}
