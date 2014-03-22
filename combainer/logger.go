package combainer

import (
	"fmt"
	"log/syslog"
)

const syslogTag = "combainer"

var logger *syslog.Writer

func init() {
	var err error
	logger, err = syslog.New(syslog.LOG_INFO|syslog.LOG_LOCAL0, syslogTag)
	if err != nil {
		panic(err)
	}
}

func LogDebug(format string, args ...interface{}) {
	logger.Debug(fmt.Sprintf(format, args...))
}

func LogInfo(format string, args ...interface{}) {
	logger.Info(fmt.Sprintf(format, args...))
}

func LogErr(format string, args ...interface{}) {
	logger.Err(fmt.Sprintf(format, args...))
}

func LogWarning(format string, args ...interface{}) {
	logger.Warning(fmt.Sprintf(format, args...))
}
