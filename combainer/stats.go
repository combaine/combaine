package combainer

import (
	"sync/atomic"
	"time"
)

type clientStats struct {
	successParsing   int64
	failedParsing    int64
	successAggregate int64
	failedAggregate  int64
	last             int64
}

func (cs *clientStats) AddSuccessParsing() {
	atomic.AddInt64(&cs.successParsing, 1)
	atomic.StoreInt64(&cs.last, time.Now().Unix())
}

func (cs *clientStats) AddFailedParsing() {
	atomic.AddInt64(&cs.failedParsing, 1)
	atomic.StoreInt64(&cs.last, time.Now().Unix())
}

func (cs *clientStats) AddSuccessAggregate() {
	atomic.AddInt64(&cs.successAggregate, 1)
	atomic.StoreInt64(&cs.last, time.Now().Unix())
}

func (cs *clientStats) AddFailedAggregate() {
	atomic.AddInt64(&cs.failedAggregate, 1)
	atomic.StoreInt64(&cs.last, time.Now().Unix())
}

func (cs *clientStats) GetStats() *StatInfo {
	return &StatInfo{
		ParsingSuccess:   atomic.LoadInt64(&cs.successParsing),
		ParsingFailed:    atomic.LoadInt64(&cs.failedParsing),
		ParsingTotal:     atomic.LoadInt64(&cs.successParsing) + atomic.LoadInt64(&cs.failedParsing),
		AggregateSuccess: atomic.LoadInt64(&cs.successAggregate),
		AggregateFailed:  atomic.LoadInt64(&cs.failedAggregate),
		AggregateTotal:   atomic.LoadInt64(&cs.successAggregate) + atomic.LoadInt64(&cs.failedAggregate),
		Heartbeated:      atomic.LoadInt64(&cs.last),
	}
}
