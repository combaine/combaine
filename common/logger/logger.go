package logger

import (
	"sync"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

var (
	log      *cocaine.Logger
	logMutex sync.Mutex
)

func LazyLoggerInitialization() (*cocaine.Logger, error) {
	var err error
	if log != nil {
		return log, nil
	} else {
		logMutex.Lock()
		defer logMutex.Unlock()
		if log != nil {
			return log, nil
		}
		log, err = cocaine.NewLogger()
		return log, err
	}
}

func Debugf(format string, data ...interface{}) (err error) {
	log, err := LazyLoggerInitialization()
	if err != nil {
		return
	}
	log.Debugf(format, data...)
	return
}

func Infof(format string, data ...interface{}) (err error) {
	log, err := LazyLoggerInitialization()
	if err != nil {
		return
	}
	log.Infof(format, data...)
	return
}

func Errf(format string, data ...interface{}) (err error) {
	log, err := LazyLoggerInitialization()
	if err != nil {
		return
	}
	log.Errf(format, data...)
	return
}

func Warnf(format string, data ...interface{}) (err error) {
	log, err := LazyLoggerInitialization()
	if err != nil {
		return
	}
	log.Warnf(format, data...)
	return
}
