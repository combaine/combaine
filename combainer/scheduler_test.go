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
	var ch chan struct{}
	cases := map[string]map[string]map[string]chan struct{}{
		"EmptyMap": make(map[string]map[string]chan struct{}),
		"FullMap": {
			"host1": {
				"c10": ch, "c11": ch, "c12": ch, "c13": ch, "c14": ch,
				"c15": ch, "c16": ch, "c17": ch, "c18": ch, "c19": ch,
				"c20": ch, "c21": ch, "c22": ch, "c23": ch, "c24": ch,
				"c25": ch, "c26": ch, "c27": ch, "c28": ch, "c29": ch,
			},
			"host2": {
				"c04": ch, "c05": ch, "c06": ch, "c07": ch, "c08": ch,
				"c77": ch, "c88": ch, "c99": ch, // faked configs
			},
			"host3": {"c01": ch, "c02": ch, "c03": ch, "c09": ch, "c30": ch},
		},
		"PartialMap": {
			"host1": {
				"c10": ch, "c11": ch, "c12": ch, "c13": ch, "c14": ch,
				"c26": ch, "c27": ch, "c28": ch, "c29": ch, "c30": ch,
			},
			"host2": {"c04": ch, "c05": ch, "c06": ch},
			"host3": {"c01": ch, "c02": ch, "c03": ch},
		},
		"OneEmptyMap": {
			"host1": {
				"c10": ch, "c11": ch, "c12": ch, "c13": ch, "c26": ch,
				"c27": ch, "c28": ch, "c29": ch, "c30": ch,
			},
			"host2": {
				"c04": ch, "c05": ch, "c06": ch, "c07": ch, "c08": ch, "c09": ch,
				// faked configs
				"c77": ch, "c88": ch, "c99": ch,
			},
			"host3": {},
		},
		"FirstFullMap": {
			"host1": {
				"c10": ch, "c11": ch, "c12": ch, "c13": ch, "c14": ch,
				"c15": ch, "c16": ch, "c17": ch, "c18": ch, "c19": ch,
				"c20": ch, "c21": ch, "c22": ch, "c23": ch, "c24": ch,
				"c25": ch, "c26": ch, "c27": ch, "c28": ch, "c29": ch,
				"c04": ch, "c05": ch, "c06": ch, "c07": ch, "c08": ch,
				"c09": ch, "c30": ch, "c01": ch, "c02": ch, "c03": ch,
			},
			"host2": {"c77": ch, "c88": ch, "c99": ch}, // faked configs
			"host3": {},
		},
	}

	repo := &dummyRepo{
		"c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10",
		"c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20",
		"c21", "c22", "c23", "c24", "c25", "c26", "c27", "c28", "c29", "c30",
	}
	cl := &Cluster{repo: repo}
	cl.log = logrus.WithField("source", "test")
	assert.NoError(t, cl.distributeTasks([]string{}))

	hosts := []string{"host1", "host2", "host3"}
	// Even configs
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

	hosts = []string{"host1", "host2"}
	// With dead nodes
	for n, c := range cases {
		t.Logf("Test with dead node for %s", n)
		cl.store = &FSMStore{store: c}
		cl.distributeTasks(hosts)
		a := len(cl.store.store["host1"])
		assert.Equal(t, a > 12, a < 17, "Test failed 12 < host1(%d) < 17 host1(%d), host2(%d), host3(%d - dead node)", a,
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

	// Odd configs
	cases = map[string]map[string]map[string]chan struct{}{
		"EmptyMap": make(map[string]map[string]chan struct{}),
		"FullMap": {
			"host1": {"c10": ch, "c11": ch, "c12": ch, "c13": ch, "c14": ch},
			"host2": {"c04": ch, "c05": ch, "c06": ch, "c07": ch, "c08": ch},
			"host3": {},
		},
	}

	repo = &dummyRepo{"c01", "c02", "c03", "c04", "c05", "c06", "c07"}
	cl = &Cluster{repo: repo}
	cl.log = logrus.WithField("source", "test")
	hosts = []string{"host1", "host2", "host3"}
	for n, c := range cases {
		t.Logf("Test for %s", n)
		cl.store = &FSMStore{store: c}
		cl.distributeTasks(hosts)
		a := len(cl.store.store["host1"])
		assert.Equal(t, a > 2, a < 5, "Test failed 3 < host1(%d) < 5 host1(%d), host2(%d), host3(%d)", a,
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
