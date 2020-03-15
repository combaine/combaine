package worker

import "flag"

// Flags are flags for worker
var Flags = struct {
	Endpoint  *string
	Tracing   *bool
	Version   *bool
	LogOutput *string
	LogLevel  *string
}{
	Endpoint:  flag.String("endpoint", ":10052", "endpoint"),
	Tracing:   flag.Bool("trace", false, "enable tracing"),
	Version:   flag.Bool("version", false, "print version and exit"),
	LogOutput: flag.String("logoutput", "/dev/stderr", "path to logfile"),
	LogLevel:  flag.String("loglevel", "info", "debug|info|warn|warning|error|panic in any case"),
}
