package logger

import (
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cocaine/cocaine-framework-go/cocaine"
)

const waitForLogger = 5 // attempts

// CocaineLog is logger with cocaine logger interface
var CocaineLog Logger

// Logger is cocaine logger interface
type Logger interface {
	Debug(data ...interface{})
	Debugf(format string, data ...interface{})
	Info(data ...interface{})
	Infof(format string, data ...interface{})
	Warn(data ...interface{})
	Warnf(format string, data ...interface{})
	Err(data ...interface{})
	Errf(format string, data ...interface{})
}

// MustCreateLogger create cocaine logger
// senders should call this function in init function
func MustCreateLogger() {
	var err error
	for i := 0; i < waitForLogger; i++ {
		CocaineLog, err = cocaine.NewLogger()
		if err == nil {
			return
		}
		log.Print("Unable to create Cocaine logger")
		time.Sleep(time.Duration(i) * time.Second)
	}
	log.Panicf("Unable to create Cocaine logger, but must %v", err)
}

type loggerLogrus struct {
	*logrus.Logger
}

func (l *loggerLogrus) Err(data ...interface{}) {
	l.Error(data...)
}
func (l *loggerLogrus) Errf(format string, data ...interface{}) {
	l.Errorf(format, data...)
}

// LocalLogger wrap logrus logger with cocaine logger interface
func LocalLogger() Logger {
	return &loggerLogrus{Logger: logrus.StandardLogger()}
}

// InitializeLogger initialize new logrus logger with rotate handler
func InitializeLogger(loglevel logrus.Level, outputPath string) {
	var (
		file          io.WriteCloser
		rotationMutex sync.Mutex
		sighupTrap    = make(chan os.Signal, 1)
	)

	formatter := &CombaineFormatter{}
	logrus.SetLevel(loglevel)
	logrus.SetFormatter(formatter)

	rotateFile := func() error {
		rotationMutex.Lock()
		defer rotationMutex.Unlock()

		if file != nil {
			file.Close()
		}

		var err error
		rawFile, err := os.OpenFile(outputPath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}

		if outputPath != "/dev/stderr" && outputPath != "/dev/stdout" {
			if _, err := rawFile.Seek(0, os.SEEK_END); err != nil {
				return err
			}
		}

		file = rawFile

		logrus.SetOutput(file)
		return nil
	}

	if err := rotateFile(); err != nil {
		log.Fatalf("unable to initialize logger %s", err)
	}

	signal.Notify(sighupTrap, syscall.SIGHUP)

	go func() {
		for {
			<-sighupTrap
			if err := rotateFile(); err != nil {
				log.Fatalf("unable to rotate output %s", err)
			}
		}
	}()
}

// LogrusLevelFlag flag for parsing logrus level
type LogrusLevelFlag logrus.Level

// Set flag
func (l *LogrusLevelFlag) Set(val string) error {
	level, err := logrus.ParseLevel(strings.ToLower(val))
	if err != nil {
		return err
	}
	(*l) = LogrusLevelFlag(level)
	return nil
}

// ToLogrusLevel convert flag
func (l *LogrusLevelFlag) ToLogrusLevel() logrus.Level {
	return logrus.Level(*l)
}

// String convert flag
func (l *LogrusLevelFlag) String() string {
	return l.ToLogrusLevel().String()
}

// Debugf format debug message
func Debugf(format string, data ...interface{}) {
	CocaineLog.Debugf(format, data...)
}

// Infof format info message
func Infof(format string, data ...interface{}) {
	CocaineLog.Infof(format, data...)
}

// Errf format error message
func Errf(format string, data ...interface{}) {
	CocaineLog.Errf(format, data...)
}

// Warnf format warning message
func Warnf(format string, data ...interface{}) {
	CocaineLog.Warnf(format, data...)
}
