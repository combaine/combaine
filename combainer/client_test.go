package combainer

import (
	"testing"
	"time"

	"github.com/noxiouz/Combaine/common/cache"
	"github.com/noxiouz/Combaine/common/configs"

	"github.com/stretchr/testify/assert"
)

const mockHosts = "DC	host1\nDC	host2"

func TestClient(t *testing.T) {
	repo, err := configs.NewFilesystemRepository("../tests/fixtures/configs")
	if err != nil {
		t.Fatalf("unable to create repo: %s", err)
	}
	cfg := repo.GetCombainerConfig()
	cacher, _ := cache.NewCache("InMemory", nil)
	ctx := Context{
		cacher,
	}

	cacher.Put("simpleFetcherCacheNamespace", "photo-proxy", []byte(mockHosts))
	cl := Client{
		Repository: repo,
		Config:     cfg,
		lockname:   "",
		cloudHosts: []string{"HOST1", "HOST2"},
		Context:    &ctx,
	}

	sp, err := cl.UpdateSessionParams("img_status")
	if err != nil {
		t.Fatalf("unable to generate sp: %s", err)
	}

	if sp.ParsingTime != 48*time.Second || sp.WholeTime != 1*time.Minute {
		t.Fatalf("time frames were calculated unproperly %s %s", sp.ParsingTime, sp.WholeTime)
	}
	for _, taskitem := range sp.PTasks {
		assert.Equal(t, "img_status", taskitem.ParsingConfigName)
		assert.Equal(t, "photo-proxy", taskitem.ParsingConfig.GetMetahost())
		assert.Equal(t, "http://overrided=%s", taskitem.ParsingConfig.HostFetcher["BasicUrl"])
		assert.Equal(t, "\t", taskitem.ParsingConfig.HostFetcher["Separator"])
		assert.Equal(t, "/timetail?pattern=img_status&log_ts=", taskitem.ParsingConfig.DataFetcher["timetail_url"])
	}
}
