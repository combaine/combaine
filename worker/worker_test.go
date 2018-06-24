package worker

import (
	"os"
	"testing"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/tests"
	"github.com/sirupsen/logrus"
)

const repoPath = "../tests/testdata/configs"

func NewService(n string, a ...interface{}) (cache.Service, error) {
	return tests.NewService(n, a...)
}

func TestMain(m *testing.M) {
	cacher = cache.NewServiceCacher(NewService)
	if err := repository.Init(repoPath); err != nil {
		logrus.Fatal(err)
	}
	os.Exit(m.Run())
}
