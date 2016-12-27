package combainer

import (
	"fmt"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common/configs"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	const repopath = "../tests/testdata/configs"

	repo, err := configs.NewFilesystemRepository(repopath)
	assert.Nil(t, err, fmt.Sprintf("Unable to create repo %s", err))

	context := &Context{
		Cache:  nil,
		Logger: logrus.StandardLogger(),
	}

	c, err := NewClient(context, repo)
	assert.Nil(t, err, fmt.Sprintf("Unable to create client %s", err))
	assert.NotEmpty(t, c.ID)
	c.Close()
}
