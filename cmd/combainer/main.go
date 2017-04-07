package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc/grpclog"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/combainer"
	"github.com/combaine/combaine/common/formatter"
)

const defaultConfigsPath = "/etc/combaine"

type logrusLevelFlag logrus.Level

func (l *logrusLevelFlag) Set(val string) error {
	level, err := logrus.ParseLevel(strings.ToLower(val))
	if err != nil {
		return err
	}
	(*l) = logrusLevelFlag(level)
	return nil
}

func (l *logrusLevelFlag) ToLogrusLevel() logrus.Level {
	return logrus.Level(*l)
}

func (l *logrusLevelFlag) String() string {
	return l.ToLogrusLevel().String()
}

var (
	endpoint    string
	profiler    string
	logoutput   string
	configsPath string
	period      uint
	active      bool
	loglevel    = logrusLevelFlag(logrus.InfoLevel)
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&configsPath, "configspath", defaultConfigsPath, "path to root of configs")
	flag.UintVar(&period, "period", 5, "period of retrying new lock (sec)")
	flag.BoolVar(&active, "active", true, "enable a distribution of tasks")
	flag.Var(&loglevel, "loglevel", "debug|info|warn|warning|error|panic in any case")

	flag.Parse()
	initializeLogger(loglevel.ToLogrusLevel(), logoutput)
	var logger grpclog.Logger = log.New(logrus.WithField("source", "grpc").Logger.Writer(), "", log.LstdFlags)
	grpclog.SetLogger(logger)
}

var (
	file          io.WriteCloser
	outputPath    string
	rotationMutex sync.Mutex
	sighupTrap    = make(chan os.Signal, 1)
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

func initializeLogger(loglevel logrus.Level, output string) {
	outputPath = output
	formatter := &formatter.CombaineFormatter{}
	logrus.SetLevel(loglevel)
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

func main() {
	cfg := combainer.CombaineServerConfig{
		ConfigsPath:  configsPath,
		Period:       time.Duration(period) * time.Second,
		RestEndpoint: endpoint,
		Active:       active,
	}

	cmb, err := combainer.New(cfg)
	if err != nil {
		logrus.Fatal(err)
	}

	if err = cmb.Serve(); err != nil {
		logrus.Fatal(err)
	}
}
