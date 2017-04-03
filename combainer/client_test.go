package combainer

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/combaine/combaine/common/configs"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	const repopath = "../tests/testdata/configs"

	repo, err := configs.NewFilesystemRepository(repopath)
	assert.Nil(t, err, fmt.Sprintf("Unable to create repo %s", err))

	c, err := NewClient(nil, nil, repo)
	assert.Nil(t, err, fmt.Sprintf("Unable to create client %s", err))
	assert.NotEmpty(t, c.ID)
}

func TestGetRandomHost(t *testing.T) {
	cases := []struct {
		hosts []string
		empty bool
	}{
		{[]string{}, true},
		{[]string{"host1", "host2", "host3", "host4", "host5", "hosts6"}, false},
	}

	for _, c := range cases {
		if c.empty {
			assert.Equal(t, "", getRandomHost(c.hosts))
		} else {
			cur, prev := "", ""
			random := 0
			for i := 0; i < 10; i++ {
				cur = getRandomHost(c.hosts)
				assert.NotEqual(t, "", cur)
				if i != 0 && cur != prev {
					random++
				}
				prev = cur
			}
			if random < 5 {
				log.Fatal("random very predictable")
			}
			log.Printf("Got %d random hosts", random)
		}
	}
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
