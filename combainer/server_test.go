package combainer

import (
	"os"
	"testing"
	"time"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/logger"
	"github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	combainerCache = cache.NewCache(1*time.Minute, 2*time.Minute, logger.FromLogrusLogger(logrus.New()))
	os.Exit(m.Run())
}
