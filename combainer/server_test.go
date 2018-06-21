package combainer

import (
	"os"
	"testing"
	"time"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/repository"
	"github.com/sirupsen/logrus"
)

const repoPath = "../tests/testdata/configs"

func TestMain(m *testing.M) {
	if err := repository.Init(repoPath); err != nil {
		logrus.Fatal(err)
	}
	combainerCache = cache.NewCache(1*time.Minute, 2*time.Minute, logger.FromLogrusLogger(logrus.New()))
	os.Exit(m.Run())
}
