package utils

import "flag"

// Flags are flags for worker
var Flags = struct {
	Endpoint *string
	Tracing  *bool
	Version  *bool
}{
	Endpoint: flag.String("endpoint", ":10052", "endpoint"),
	Tracing:  flag.Bool("trace", false, "enable tracing"),
	Version:  flag.Bool("version", false, "print version and exit"),
}
