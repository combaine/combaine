package logger

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/cocaine/cocaine-framework-go/cocaine"
)

var (
	Log Logger = mustCreateLogger()
)

type Logger interface {
	Debugf(format string, data ...interface{})
	Infof(format string, data ...interface{})
	Warnf(format string, data ...interface{})
	Errf(format string, data ...interface{})
}

type loggerLogrus struct {
	*logrus.Logger
}

func (l *loggerLogrus) Errf(format string, data ...interface{}) {
	l.Errorf(format, data...)
}

func mustCreateLogger() Logger {
	log, err := cocaine.NewLogger()
	if err != nil {
		// panic(fmt.Sprintf("Unable to create Cocaine logger, but must %v", err))
		return &loggerLogrus{Logger: logrus.StandardLogger()}
	}
	return log
}

func MustCreateService(name string) *cocaine.Service {
	service, err := cocaine.NewService(name)
	if err != nil {
		panic(fmt.Sprintf("Unable to create Cocaine service %s, but must %v", name, err))
	}
	return service
}

func Debugf(format string, data ...interface{}) {
	Log.Debugf(format, data...)
}

func Infof(format string, data ...interface{}) {
	Log.Infof(format, data...)
}

func Errf(format string, data ...interface{}) {
	Log.Errf(format, data...)
}

func Warnf(format string, data ...interface{}) {
	Log.Warnf(format, data...)
}
