package combainer

import (
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
)

const syslogTag = "combainer"

var (
	severityMap = map[string]logrus.Level{
		"DEBUG": logrus.DebugLevel,
		"INFO":  logrus.InfoLevel,
		"WARN":  logrus.WarnLevel,
		"ERROR": logrus.ErrorLevel,
	}

	// logoutput
	file       io.WriteCloser
	outputPath string

	LogDebug   = logrus.Debugf
	LogInfo    = logrus.Infof
	LogWarning = logrus.Warningf
	LogErr     = logrus.Errorf

	rotationMutex sync.Mutex

	sighupTrap = make(chan os.Signal, 1)
)

func rotateFile() error {
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

func InitializeLogger(loglevel, output string) {
	if lvl, ok := severityMap[loglevel]; ok {
		logrus.SetLevel(lvl)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}

	outputPath = output
	formatter := &logrus.TextFormatter{
		DisableColors: true,
	}
	logrus.SetFormatter(formatter)
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
