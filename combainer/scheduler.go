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
				cmd := FSMCommand{Type: cmdRemoveConfig, Host: host, Config: cfg}
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
		cmd := FSMCommand{Type: cmdAssignConfig, Host: host, Config: cfg}
		if err := c.raftApply(cmd); err != nil {
			return errors.Wrapf(err, "Failed to assign config '%s' to host '%s'", cfg, host)
		}
	}
	return nil
}

func (c *FSM) handleTask(config string, stopCh chan struct{}) {
	var iteration uint64
	log := c.log.WithField("config", config)

RECLIENT:
	cl, err := NewClient(c.cache, c.repo)
	if err != nil {
		log.Errorf("can't create client %s", err)
		time.Sleep(c.updateInterval)
		goto RECLIENT
	}
	GlobalObserver.RegisterClient(cl, config)
	defer GlobalObserver.UnregisterClient(config)

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		iteration++
		hosts := (*Cluster)(c).Hosts()
		if err = cl.Dispatch(strconv.FormatUint(iteration, 10), hosts, config, genUniqueID, shouldWait); err != nil {
			log.WithField("iteration", strconv.FormatUint(iteration, 10)).Errorf("Dispatch error %s", err)
			time.Sleep(c.updateInterval)
		}
	}
}
