package logger

import (
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var fields = logrus.Fields{
	"foo":   "bar",
	"baz":   "qux",
	"one":   "two",
	"three": "four",
}

var entry = &logrus.Entry{
	Time:    time.Now(),
	Level:   logrus.InfoLevel,
	Message: "message",
	Data:    fields,
}

func TestMain(t *testing.T) {
	f := CombaineFormatter{}

	cases := []struct {
		level  logrus.Level
		expect string
	}{
		{logrus.DebugLevel, "DEBUG"},
		{logrus.InfoLevel, "INFO"},
		{logrus.WarnLevel, "WARN"},
		{logrus.ErrorLevel, "ERROR"},
		{logrus.FatalLevel, "ERROR"},
		{logrus.PanicLevel, "PANIC"},
		{33, "unknown"},
	}
	for _, c := range cases {
		entry.Level = c.level
		r, _ := f.Format(entry)
		assert.Contains(t, string(r), c.expect)
		t.Logf("%s", r)
	}
}

func BenchmarkFormatter(b *testing.B) {
	formatter := CombaineFormatter{}
	for i := 0; i < b.N; i++ {
		d, err := formatter.Format(entry)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(d)))
	}
}
