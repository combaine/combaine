package logger

import (
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/grpclog"
)

// InitializeLogger initialize new logrus logger with rotate handler
func InitializeLogger() {
	var (
		file          io.WriteCloser
		rotationMutex sync.Mutex
		sighupTrap    = make(chan os.Signal, 1)
	)

	formatter := &CombaineFormatter{}

	lvl, err := logrus.ParseLevel(*LogLevel)
	if err != nil {
		logrus.Fatalf("failed to parse loglevel: %v", err)
	}
	logrus.SetLevel(lvl)
	logrus.SetFormatter(formatter)

	rotateFile := func() error {
		rotationMutex.Lock()
		defer rotationMutex.Unlock()

		if file != nil {
			file.Close()
		}

		var err error
		rawFile, err := os.OpenFile(*LogOutput, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}

		if *LogOutput != "/dev/stderr" && *LogOutput != "/dev/stdout" {
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

	grpclog.SetLoggerV2(NewLoggerV2WithVerbosity(0))
}
