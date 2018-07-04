package combainer

import (
	"os"
	"testing"
	"time"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/repository"
	"github.com/sirupsen/logrus"
)

const repoPath = "../tests/testdata/configs"

func TestMain(m *testing.M) {
	if err := repository.Init(repoPath); err != nil {
		logrus.Fatal(err)
	}
	combainerCache = cache.NewCache(1*time.Minute, 2*time.Minute, 5*time.Minute)
	os.Exit(m.Run())
}
