package combainer

import (
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common/configs"
	"github.com/stretchr/testify/assert"
)

type dummyRepo []string

func (d *dummyRepo) ListParsingConfigs() ([]string, error) { return []string(*d), nil }
func (d *dummyRepo) GetAggregationConfig(name string) (c configs.EncodedConfig, e error) {
	return c, e
}
func (d *dummyRepo) GetParsingConfig(name string) (c configs.EncodedConfig, e error) {
	return c, e
}
func (d *dummyRepo) GetCombainerConfig() (c configs.CombainerConfig) { return c }
func (d *dummyRepo) ParsingConfigIsExists(name string) bool          { return true }
func (d *dummyRepo) ListAggregationConfigs() (l []string, e error)   { return l, e }

func TestDistributeTasks(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	assignConfig = func(c *Cluster, host, config string) error {
		t.Logf("Assign config to host %s:%s", host, config)
		c.store.Put(host, config)
		return nil
	}
	releaseConfig = func(c *Cluster, host, config string) error {
		t.Logf("Release config from host %s:%s", host, config)
		c.store.Remove(host, config)
		return nil
	}
	repo := &dummyRepo{
		"c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10",
		"c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20",
		"c21", "c22", "c23", "c24", "c25", "c26", "c27", "c28", "c29", "c30",
	}
	hosts := []string{"host1", "host2", "host3"}
	type ch chan struct{}

	cl := &Cluster{repo: repo}
	cl.log = logrus.WithField("source", "test")
	cases := map[string]map[string]map[string]chan struct{}{
		"EmptyMap": make(map[string]map[string]chan struct{}),
		"FullMap": map[string]map[string]chan struct{}{
			"host1": map[string]chan struct{}{
				"c10": make(ch), "c11": make(ch), "c12": make(ch), "c13": make(ch),
				"c14": make(ch), "c15": make(ch), "c16": make(ch), "c17": make(ch),
				"c18": make(ch), "c19": make(ch), "c20": make(ch), "c21": make(ch),
				"c22": make(ch), "c23": make(ch), "c24": make(ch), "c25": make(ch),
				"c26": make(ch), "c27": make(ch), "c28": make(ch), "c29": make(ch),
				"c30": make(ch),
			},
			"host2": map[string]chan struct{}{
				"c04": make(ch), "c05": make(ch), "c06": make(ch),
				"c07": make(ch), "c08": make(ch), "c09": make(ch),
				"c77": make(ch), "c88": make(ch), "c99": make(ch),
			},
			"host3": map[string]chan struct{}{
				"c01": make(ch), "c02": make(ch), "c03": make(ch),
			},
		},
	}

	for n, c := range cases {
		t.Logf("Test for %s", n)
		cl.store = &FSMStore{store: c}
		cl.distributeTasks(hosts)
		a := len(cl.store.store["host1"])
		assert.Equal(t, a > 8, a < 12, "Test failed 8 < host1(%d) < 12 host1(%d), host2(%d), host3(%d)", a,
			len(cl.store.store["host1"]),
			len(cl.store.store["host2"]),
			len(cl.store.store["host3"]),
		)
		configSet := make(map[string]string)
		for h := range cl.store.store {
			for cfg := range cl.store.store[h] {
				assert.True(t, configSet[cfg] == "", "Dubilcate dispatching %s for %s and %s", cfg, h, configSet[cfg])
				configSet[cfg] = h
			}
		}
	}
}
