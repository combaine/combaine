package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/Sirupsen/logrus"

	"github.com/combaine/combaine/combainer/server"
	"github.com/combaine/combaine/common/formatter"
)

const (
	defaultConfigsPath = "/etc/combaine"
)

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
	ConfigsPath string
	period      uint
	active      bool
	loglevel    logrusLevelFlag = logrusLevelFlag(logrus.InfoLevel)
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&ConfigsPath, "configspath", defaultConfigsPath, "path to root of configs")
	flag.UintVar(&period, "period", 5, "period of retrying new lock (sec)")
	flag.BoolVar(&active, "active", true, "enable a distribution of tasks")
	flag.Var(&loglevel, "loglevel", "debug|info|warn|warning|error|panic in any case")
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
	flag.Parse()
	initializeLogger(loglevel.ToLogrusLevel(), logoutput)
	cfg := server.CombaineServerConfig{
		ConfigsPath:  ConfigsPath,
		Period:       time.Duration(period) * time.Second,
		RestEndpoint: endpoint,
		Active:       active,
	}

	cmb, err := server.NewCombainer(cfg)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()
	cmb.Serve()
}
