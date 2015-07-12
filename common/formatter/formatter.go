package formatter

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/Sirupsen/logrus"
)

// const format = "02/01/2006 15:04:05"
const format = time.RFC3339

type CombaineFormatter struct{}

func getLevel(lvl logrus.Level) string {
	switch lvl {
	case logrus.DebugLevel:
		return "DEBUG"
	case logrus.InfoLevel:
		return "INFO"
	case logrus.WarnLevel:
		return "WARN"
	case logrus.ErrorLevel, logrus.FatalLevel:
		return "ERROR"
	default:
		return lvl.String()
	}
}

func (f *CombaineFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var keys = make([]string, 0, len(entry.Data))
	for key := range entry.Data {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	buf := new(bytes.Buffer)

	buf.WriteString(entry.Time.Format(format))
	buf.WriteByte('\t')
	buf.WriteString(getLevel(entry.Level))
	buf.WriteByte('\t')
	buf.WriteString(entry.Message)
	buf.WriteByte('\t')
	buf.WriteByte('[')

	var i = len(entry.Data)
	for _, k := range keys {
		buf.WriteString(fmt.Sprintf("%s: %s", k, entry.Data[k]))
		i--
		if i > 0 {
			buf.WriteByte(',')
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte(']')
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}
