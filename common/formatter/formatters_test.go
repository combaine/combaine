package formatter

import (
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
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
	r, _ := f.Format(entry)
	t.Logf("%s", r)
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
