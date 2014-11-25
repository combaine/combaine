package combainer

import (
	"crypto/md5"
	"fmt"
	"io"
	"time"
)

const (
	CONFIGS_PARSING_PATH     = "/etc/combaine/parsing/"
	CONFIGS_AGGREGATION_PATH = "/etc/combaine/aggregate"
	COMBAINER_PATH           = "/etc/combaine/combaine.yaml"
)

//Various utility functions
func GenerateSessionId(lockname string, start, deadline *time.Time) string {
	h := md5.New()
	io.WriteString(h, (fmt.Sprintf("%s%d%d", lockname, *start, *deadline)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func GenerateSessionTimeFrame(sessionDuration uint) (time.Duration, time.Duration) {
	parsingTime := time.Duration(float64(sessionDuration)*0.8) * time.Second
	wholeTime := time.Duration(sessionDuration) * time.Second
	return parsingTime, wholeTime
}
