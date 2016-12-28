package combainer

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStat(t *testing.T) {
	s := clientStats{}
	s.AddSuccessAggregate()
	assert.EqualValues(t, s.successAggregate, 1)
	s.AddFailedAggregate()
	assert.EqualValues(t, s.failedAggregate, 1)
	stats := s.GetStats()
	assert.EqualValues(t, stats.AggregateTotal, 2)

	s.AddSuccessParsing()
	assert.EqualValues(t, s.successParsing, 1)
	s.AddFailedParsing()
	assert.EqualValues(t, s.failedParsing, 1)
	stats = s.GetStats()
	assert.EqualValues(t, stats.ParsingFailed, 2)
	for i := 0; i < 10; i++ {
		go s.AddSuccessParsing()
	}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			s.AddSuccessParsing()
			wg.Done()
		}()
		s.AddSuccessParsing()

		wg.Add(1)
		go func() {
			s.AddFailedParsing()
			wg.Done()
		}()
		s.AddFailedConnectParsing()
	}
	wg.Wait()

	stats = s.GetStats()
	assert.EqualValues(t, stats.ParsingTotal, 4012)
}
