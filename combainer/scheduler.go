package combainer

import (
	"strconv"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/pkg/errors"
)

var (
	shouldWait  = true
	genUniqueID = ""
)

func (c *Cluster) distributeTasks() error {
	configs, err := c.repo.ListParsingConfigs()
	if err != nil {
		return errors.Wrap(err, "Failed to list parsing config")
	}
	configSet := make(map[string]struct{}, len(configs))
	for _, c := range configs {
		configSet[c] = struct{}{}
	}

	hosts := c.Hosts()
	for _, host := range hosts {
		for _, cfg := range c.store.List(host) {
			if _, ok := configSet[cfg]; ok {
				delete(configSet, cfg)
			} else {
				cmd := fsmCommand{Type: cmdRemoveConfig, Host: host, Config: cfg}
				if err := c.raftApply(cmd); err != nil {
					return errors.Wrapf(err, "Failed to remove config '%s' from host '%s'", cfg, host)
				}
			}
		}
	}
	randomize := false
	if len(configSet) < len(hosts) {
		randomize = true
	}
	var next int
	var host string
	for cfg := range configSet {
		if randomize {
			host = common.GetRandomString(hosts)
		} else {
			host = hosts[next%len(hosts)]
			next++
		}
		cmd := fsmCommand{Type: cmdAddConfig, Host: host, Config: cfg}
		if err := c.raftApply(cmd); err != nil {
			return errors.Wrapf(err, "Failed to assign config '%s' to host '%s'", cfg, host)
		}
	}
	return nil
}

func (c *fsm) handleTask(config string, stopCh chan struct{}) {
	var iteration uint64
	log := c.log.WithField("config", config)

RECLIENT:
	cl, err := NewClient(c.cache, c.repo)
	if err != nil {
		log.Errorf("can't create client %s", err)
		time.Sleep(c.updateInterval)
		goto RECLIENT
	}
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		iteration++
		ilog := log.WithField("iteration", strconv.FormatUint(iteration, 10))
		hosts := (*Cluster)(c).Hosts()
		if err = cl.Dispatch(hosts, config, genUniqueID, shouldWait); err != nil {
			ilog.Errorf("Dispatch error %s", err)
			time.Sleep(c.updateInterval)
		}
	}
}
