package combainer

import (
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
	assert.EqualValues(t, stats.ParsingTotal, 2)
	for i := 0; i < 10; i++ {
		go s.AddSuccessParsing()
	}
	for i := 0; i < 10; i++ {
		go s.AddSuccessParsing()
		s.AddFailedParsing()
	}
	for i := 0; i < 10; i++ {
		stats = s.GetStats()
	}
	_ = stats
}
