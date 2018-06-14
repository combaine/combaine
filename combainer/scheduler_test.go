package combainer

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/combaine/combaine/repository"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func newTestRepo(configs []string) func() {
	dir, err := ioutil.TempDir("", "test_repo")
	if err != nil {
		logrus.Fatal(err)
	}
	parsingDir := filepath.Join(dir, "parsing")
	aggDir := filepath.Join(dir, "aggregate")
	os.Mkdir(parsingDir, 0777)
	os.Mkdir(aggDir, 0777)

	for _, name := range configs {
		pCfg := filepath.Join(parsingDir, name+".yaml")
		aCfg := filepath.Join(aggDir, name+".yaml")
		if err := ioutil.WriteFile(pCfg, []byte(""), 0666); err != nil {
			logrus.Fatal(err)
		}
		if err := ioutil.WriteFile(aCfg, []byte(""), 0666); err != nil {
			logrus.Fatal(err)
		}
	}
	mainConfig := filepath.Join(dir, "combaine.yaml")
	if err := ioutil.WriteFile(mainConfig, []byte(""), 0666); err != nil {
		logrus.Fatal(err)
	}
	err = repository.InitFilesystemRepository(dir)
	if err != nil {
		logrus.Fatal(err)
	}
	list, err := repository.ListParsingConfigs()
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.Print("New repo content", list)
	return func() { os.RemoveAll(dir) }
}

// PushParsingConfig add new parsing config
func pushParsingConfig(name string) {
	cfg := filepath.Join(repository.GetBasePath(), "parsing", name+".yaml")
	if err := ioutil.WriteFile(cfg, []byte(""), 0666); err != nil {
		logrus.Fatal(err)
	}
}

func TestDistributeTasks(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	assignConfig = func(c *Cluster, host, config string) (err error) {
		cmd := FSMCommand{Type: cmdAssignConfig, Host: host, Config: config}
		log := &raft.Log{}
		if log.Data, err = json.Marshal(cmd); err != nil {
			return errors.Wrapf(err, "Failed to assign config '%s' to host '%s'", config, host)
		}
		(*FSM)(c).Apply(log)
		return err
	}

	releaseConfig = func(c *Cluster, host, config string) (err error) {
		cmd := FSMCommand{Type: cmdRemoveConfig, Host: host, Config: config}
		log := &raft.Log{}
		if log.Data, err = json.Marshal(cmd); err != nil {
			return errors.Wrapf(err, "Failed to release config '%s' from host '%s'", config, host)
		}
		(*FSM)(c).Apply(log)
		return err
	}
	//assignConfig = func(c *Cluster, host, config string) error {
	//	t.Logf("Assign config to host %s:%s", host, config)
	//	c.store.Put(host, config)
	//	return nil
	//}
	//releaseConfig = func(c *Cluster, host, config string) error {
	//	t.Logf("Release config from host %s:%s", host, config)
	//	c.store.Remove(host, config)
	//	return nil
	//}

	var ch chan struct{}
	cases := map[string]map[string]map[string]chan struct{}{
		"EmptyMapEven": make(map[string]map[string]chan struct{}),
		"FullMapEven": {
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

	cleanup := newTestRepo([]string{
		"c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10",
		"c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c20",
		"c21", "c22", "c23", "c24", "c25", "c26", "c27", "c28", "c29", "c30",
	})
	defer cleanup()
	cl := &Cluster{Name: "host3", updateInterval: 3600 * time.Hour, store: NewFSMStore()}
	cl.log = logrus.WithField("source", "test")
	assert.NoError(t, cl.distributeTasks([]string{}))

	hosts := []string{"host1", "host2", "host3"}
	// Even configs
	for n, c := range cases {
		logrus.Infof("Test for %s", n)
		cl.store.Replace(c)
		cl.distributeTasks(hosts)
		a := len(cl.store.store["host1"])
		assert.Equal(t, a > 8, a < 12, "Test failed for %s, 8 < host1(%d) < 12 store: %v",
			n, a,
			cl.store.DistributionStatistic(),
		)
		configSet := make(map[string]string)
		for h := range cl.store.store {
			for cfg := range cl.store.store[h] {
				assert.True(t, configSet[cfg] == "", "Dublicate dispatching %s for %s and %s", cfg, h, configSet[cfg])
				configSet[cfg] = h
			}
		}

		if n == "FullMapEven" {
			logrus.Info("Trye dispatching new config on fully balanced cluster")
			pushParsingConfig("c55")
			cl.distributeTasks(hosts)
			present := 0
			for h := range cl.store.store {
				if _, ok := cl.store.store[h]["c55"]; ok {
					present++
				}
			}
			assert.True(t, present == 1, "Failed to dispatch new config c55")
		}
	}

	hosts = []string{"host1", "host2"}
	// With dead nodes
	for n, c := range cases {
		logrus.Infof("Test with dead node for %s", n)
		cl.store.Replace(c)
		cl.distributeTasks(hosts)
		a := len(cl.store.store["host1"])
		assert.Equal(t, a > 12, a < 17, "Test failed for %s: 12 < host1(%d) < 17 store: %v",
			n, a,
			cl.store.DistributionStatistic(),
		)
		assert.Equal(t, 0, len(cl.store.store["host3"]), "Dead node has configs")

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
		"EmptyMapOdd": make(map[string]map[string]chan struct{}),
		"FullMapOdd": {
			"host1odd": {"c10": ch, "c11": ch, "c12": ch, "c13": ch, "c14": ch},
			"host2odd": {"c04": ch, "c05": ch, "c06": ch, "c07": ch, "c08": ch},
			"host3odd": {},
		},
	}

	cleanup = newTestRepo([]string{"c01", "c02", "c03", "c04", "c05", "c06", "c07"})
	defer cleanup()
	hosts = []string{"host1odd", "host2odd", "host3odd"}
	for n, c := range cases {
		t.Logf("Test for %s", n)
		cl.store.Replace(c)
		cl.distributeTasks(hosts)
		a := len(cl.store.store["host1odd"])
		assert.Equal(t, a > 2, a < 5, "Test failed for %s 3 < host1odd(%d) < 5 host1odd(%d), host2odd(%d), host3odd(%d)",
			n, a,
			len(cl.store.store["host1odd"]),
			len(cl.store.store["host2odd"]),
			len(cl.store.store["host3odd"]),
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
