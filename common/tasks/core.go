package tasks

import (
	"fmt"
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
	Host     string
	Config   string
	Group    string
	Metahost string
}

func (t *ParsingTask) String() string {
	return fmt.Sprintf("%v", t)
}

type AggregationTask struct {
	CommonTask
	Config   string
	PConfig  string
	Group    string
	Metahost string
}
