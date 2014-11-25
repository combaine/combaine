package combainer

import (
	"sync"
	"time"
)

type clientStats struct {
	sync.RWMutex
	successParsing   int
	failedParsing    int
	successAggregate int
	failedAggregate  int
	last             int64
}

func (cs *clientStats) AddSuccessParsing() {
	cs.Lock()
	cs.successParsing++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) AddFailedParsing() {
	cs.Lock()
	cs.failedParsing++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) AddSuccessAggregate() {
	cs.Lock()
	cs.successAggregate++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) AddFailedAggregate() {
	cs.Lock()
	cs.failedAggregate++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) GetStats() (info *StatInfo) {
	cs.RLock()
	// var success = cs.success
	// var failed = cs.failed
	defer cs.RUnlock()
	info = &StatInfo{
		ParsingSuccess:   cs.successParsing,
		ParsingFailed:    cs.failedParsing,
		ParsingTotal:     cs.successParsing + cs.failedParsing,
		AggregateSuccess: cs.successAggregate,
		AggregateFailed:  cs.failedAggregate,
		AggregateTotal:   cs.successAggregate + cs.failedAggregate,
		Heartbeated:      cs.last,
	}
	return
}
