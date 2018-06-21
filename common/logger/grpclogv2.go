package logger

import (
	"log"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/grpclog"
)

// from "google.golang.org/grpc/grpclog/loggerv2.go"

type loggerT struct {
	m []*log.Logger
	v int
}

const (
	infoLog int = iota
	warningLog
	errorLog
	fatalLog
)

// severityName contains the string representation of each severity.
var severityName = []string{
	infoLog:    "grpc info:",
	warningLog: "grpc warn:",
	errorLog:   "grpc error:",
	fatalLog:   "grpc fatal",
}

// NewLoggerV2WithVerbosity ...
func NewLoggerV2WithVerbosity(v int) grpclog.LoggerV2 {
	out := logrus.StandardLogger().Writer()
	var m []*log.Logger
	m = append(m, log.New(out, severityName[infoLog], log.Ltime))
	m = append(m, log.New(out, severityName[warningLog], log.Ltime))
	m = append(m, log.New(out, severityName[errorLog], log.Ltime))
	m = append(m, log.New(out, severityName[fatalLog], log.Ltime))
	return &loggerT{m: m, v: v}
}

func (g *loggerT) Info(args ...interface{}) {
	g.m[infoLog].Print(args...)
}

func (g *loggerT) Infoln(args ...interface{}) {
	g.m[infoLog].Println(args...)
}

func (g *loggerT) Infof(format string, args ...interface{}) {
	g.m[infoLog].Printf(format, args...)
}

func (g *loggerT) Warning(args ...interface{}) {
	g.m[warningLog].Print(args...)
}

func (g *loggerT) Warningln(args ...interface{}) {
	g.m[warningLog].Println(args...)
}

func (g *loggerT) Warningf(format string, args ...interface{}) {
	g.m[warningLog].Printf(format, args...)
}

func (g *loggerT) Error(args ...interface{}) {
	g.m[errorLog].Print(args...)
}

func (g *loggerT) Errorln(args ...interface{}) {
	g.m[errorLog].Println(args...)
}

func (g *loggerT) Errorf(format string, args ...interface{}) {
	g.m[errorLog].Printf(format, args...)
}

func (g *loggerT) Fatal(args ...interface{}) {
	g.m[fatalLog].Fatal(args...)
	// No need to call os.Exit() again because log.Logger.Fatal() calls os.Exit().
}

func (g *loggerT) Fatalln(args ...interface{}) {
	g.m[fatalLog].Fatalln(args...)
	// No need to call os.Exit() again because log.Logger.Fatal() calls os.Exit().
}

func (g *loggerT) Fatalf(format string, args ...interface{}) {
	g.m[fatalLog].Fatalf(format, args...)
	// No need to call os.Exit() again because log.Logger.Fatal() calls os.Exit().
}

func (g *loggerT) V(l int) bool {
	return l <= g.v
}
