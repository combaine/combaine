package combainer

import (
	"os"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/logger"
)

func TestMain(m *testing.M) {
	combainerCache = cache.NewCache(1*time.Minute, 2*time.Minute, logger.FromLogrusLogger(logrus.New()))
	os.Exit(m.Run())
}
