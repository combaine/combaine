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
	sPar := atomic.LoadInt64(&cs.successParsing)
	fPar := atomic.LoadInt64(&cs.failedParsing)
	sAgg := atomic.LoadInt64(&cs.successAggregate)
	fAgg := atomic.LoadInt64(&cs.failedAggregate)
	return &StatInfo{
		ParsingSuccess:   sPar,
		ParsingFailed:    fPar,
		ParsingTotal:     sPar + fPar,
		AggregateSuccess: sAgg,
		AggregateFailed:  fAgg,
		AggregateTotal:   sAgg + fAgg,
		Heartbeated:      atomic.LoadInt64(&cs.last),
	}
}

func (cs *clientStats) CopyStats(to *clientStats) {
	atomic.StoreInt64(&to.successParsing, atomic.LoadInt64(&cs.successParsing))
	atomic.StoreInt64(&to.failedParsing, atomic.LoadInt64(&cs.failedParsing))
	atomic.StoreInt64(&to.successAggregate, atomic.LoadInt64(&cs.successAggregate))
	atomic.StoreInt64(&to.failedAggregate, atomic.LoadInt64(&cs.failedAggregate))
}
