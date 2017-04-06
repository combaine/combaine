package combainer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/combaine/combaine/common/configs"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	const repopath = "../tests/testdata/configs"

	repo, err := configs.NewFilesystemRepository(repopath)
	assert.Nil(t, err, fmt.Sprintf("Unable to create repo %s", err))

	c, err := NewClient(nil, repo)
	assert.Nil(t, err, fmt.Sprintf("Unable to create client %s", err))
	assert.NotEmpty(t, c.ID)
}

func TestDialContext(t *testing.T) {
	cases := []struct {
		hosts    []string
		expected string
		empty    bool
	}{
		{[]string{}, "empty list of hosts", true},
		{[]string{"host1", "host2", "host3", "host4", "host5", "hosts6"},
			"context deadline exceeded", false},
	}

	for _, c := range cases {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
		gClient, err := dialContext(ctx, c.hosts)
		if c.empty {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), c.expected)
		} else {
			assert.Nil(t, gClient)
			assert.Contains(t, err.Error(), c.expected)
		}
		cancel()
	}
}
