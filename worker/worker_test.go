package worker

import (
	"log"
	"os"
	"testing"

	"github.com/combaine/combaine/repository"
)

const repoPath = "../testdata/configs"

func TestMain(m *testing.M) {
	if err := repository.Init(repoPath); err != nil {
		log.Fatal(err)
	}
	os.Exit(m.Run())
}
