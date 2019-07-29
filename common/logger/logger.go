package logger

import (
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

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
