package common

import (
	"fmt"
)

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
