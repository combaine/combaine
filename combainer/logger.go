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
	severity_map = map[string]logrus.Level{
		"DEBUG": logrus.DebugLevel,
		"INFO":  logrus.InfoLevel,
		"WARN":  logrus.WarnLevel,
		"ERROR": logrus.ErrorLevel,
	}

	// logoutput
	file        io.WriteCloser
	output_path string

	// compatibility mappings
	LogDebug   = logrus.Debugf
	LogInfo    = logrus.Infof
	LogWarning = logrus.Warningf
	LogErr     = logrus.Errorf

	rotation_mutex sync.Mutex

	sighup_trap = make(chan os.Signal, 1)
)

func rotateFile() error {
	rotation_mutex.Lock()
	defer rotation_mutex.Unlock()

	if file != nil {
		file.Close()
	}

	var err error
	raw_file, err := os.OpenFile(output_path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	if output_path != "/dev/stderr" && output_path != "/dev/stdout" {
		if _, err := raw_file.Seek(0, os.SEEK_END); err != nil {
			return err
		}
	}

	file = raw_file

	logrus.SetOutput(file)
	return nil
}

func InitializeLogger(loglevel, output string) {
	if lvl, ok := severity_map[loglevel]; ok {
		logrus.SetLevel(lvl)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}

	output_path = output
	formatter := &logrus.TextFormatter{
		DisableColors: true,
	}
	logrus.SetFormatter(formatter)
	if err := rotateFile(); err != nil {
		log.Fatalf("unable to initialize logger %s", err)
	}

	signal.Notify(sighup_trap, syscall.SIGHUP)

	go func() {
		for {
			<-sighup_trap
			if err := rotateFile(); err != nil {
				log.Fatalf("unable to rotate output %s", err)
			}
		}
	}()
}
