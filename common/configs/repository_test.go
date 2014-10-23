package configs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {
	const (
		repopath = "../../tests/fixtures/configs"
	)
	repo, err := NewFilesystemRepository(repopath)
	if !assert.Nil(t, err, "Unable to create repo %s", err) {
		t.Fatal()
	}
	var expectedPcfg = []string{"img_status.json"}
	var expectedAggcfg = []string{"http_ok.yaml"}
	assert.Equal(t, expectedPcfg, repo.ListParsingConfigs(), "")
	assert.Equal(t, expectedAggcfg, repo.ListAggregationConfigs(), "")
}
