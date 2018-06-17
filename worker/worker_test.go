package worker

import (
	"os"
	"testing"

	"github.com/combaine/combaine/repository"
	"github.com/sirupsen/logrus"
)

const repoPath = "../tests/testdata/configs"

func TestMain(m *testing.M) {
	if err := repository.Init(repoPath); err != nil {
		logrus.Fatal(err)
	}
	os.Exit(m.Run())
}
