package logger

import "flag"

var LogOutput = flag.String("logoutput", "/dev/stderr", "path to logfile")
var LogLevel = flag.String("loglevel", "info", "debug|info|warn|warning|error|panic in any case")
