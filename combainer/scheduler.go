package combainer

import (
	"log"
	"sort"
	"time"

	"github.com/combaine/combaine/repository"
	"github.com/pkg/errors"
)

var (
	shouldWait  = true
	genUniqueID = ""
)

type balance struct {
	hosts     []string
	qty       map[string]int
	mean      int
	remainder int
}

func (b *balance) Len() int           { return len(b.hosts) }
func (b *balance) Less(i, j int) bool { return b.qty[b.hosts[i]] < b.qty[b.hosts[j]] }
func (b *balance) Swap(i, j int)      { b.hosts[i], b.hosts[j] = b.hosts[j], b.hosts[i] }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func markDeadNodes(alive []string, stats [][2]string) ([]string, [][2]string) {
	var (
		found     bool
		deadNodes []string
	)
	log.Printf("DEBUGGGGG %#v, %#v", stats, alive)
	for i := range stats {
		found = false
		for _, h := range alive {
			if h == stats[i][0] {
				found = true
			}
		}
		if !found {
			deadNodes = append(deadNodes, stats[i][0])
			stats[i][0] += " (dead)"
		}
	}
	return deadNodes, stats
}

func (c *Cluster) distributeTasks(hosts []string) error {
	configs, err := repository.ListParsingConfigs()
	c.log.Debugf("scheduler: Distribute %d tasks to %+v", len(configs), hosts)
	if err != nil {
		return errors.Wrap(err, "Failed to list parsing config")
	}
	configSet := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		configSet[cfg] = struct{}{}
	}

	clusterSize := len(hosts)
	if clusterSize == 0 {
		c.log.Warnf("scheduler: cluster is empty, there is nowhere to distribute configs")
		return nil
	}
	curStat := c.store.DistributionStatistic()
	deadNodes, curStat := markDeadNodes(hosts, curStat)
	c.log.Debugf("scheduler: Current FSM store stats %v, total %d", curStat, len(configSet))

	// Release configs from deadNodes
	for _, host := range deadNodes {
		for _, cfg := range c.store.List(host) {
			if err := releaseConfig(c, host, cfg); err != nil {
				return err
			}
		}
	}

	state := &balance{
		hosts:     hosts,
		qty:       make(map[string]int, clusterSize),
		mean:      len(configs) / clusterSize,
		remainder: len(configs) % clusterSize,
	}

	for _, host := range state.hosts {
		for _, cfg := range c.store.List(host) {
			if _, ok := configSet[cfg]; ok {
				state.qty[host]++
				delete(configSet, cfg)
			} else {
				c.log.Infof("scheduler: Release missing config %s", cfg)
				if err := releaseConfig(c, host, cfg); err != nil {
					return err
				}
			}
		}
	}

	sort.Sort(state)

	if err := c.runBalancer(state, configSet, clusterSize-1); err != nil {
		return errors.Wrap(err, "Balancer error")
	}

	curStat = c.store.DistributionStatistic()
	_, curStat = markDeadNodes(hosts, curStat)
	c.log.Infof("scheduler: Rebalanced FSM store stats %v", curStat)
	return nil
}

func (c *Cluster) runBalancer(state *balance, configSet map[string]struct{}, overloadedIndex int) error {
	var (
		wantage        int
		overloadedHost = state.hosts[overloadedIndex]
	)
	for _, host := range state.hosts {
		wantage = state.mean - state.qty[host]
		setLen := len(configSet)

		if state.remainder > 0 {
			wantage++
			state.remainder--
		}
		c.log.Infof("scheduler: Rebalance host %s (wantage %d, has %d, total %d)", host, wantage, state.qty[host], setLen)

		// rebalance assigned configs
		if state.qty[overloadedHost]-state.mean <= 0 {
			if overloadedIndex >= 0 {
				overloadedHost = state.hosts[overloadedIndex]
				overloadedIndex--
			}
		}
		toRelase := min(state.qty[overloadedHost]-state.mean, wantage)
		if toRelase > 0 {
			for _, cfg := range c.store.List(overloadedHost) {
				toRelase--
				if toRelase < 0 {
					break
				}
				if err := releaseConfig(c, overloadedHost, cfg); err != nil {
					return err
				}
				configSet[cfg] = struct{}{}
				state.qty[overloadedHost]--
			}
		}

		// distribute free configs
		toAdd := min(len(configSet), wantage)
		configsToAssign := make([]string, toAdd)
		for cfg := range configSet {
			toAdd--
			configsToAssign[toAdd] = cfg
			if toAdd <= 0 {
				break
			}
		}
		for _, cfg := range configsToAssign {
			if err := assignConfig(c, host, cfg); err != nil {
				return err
			}
			delete(configSet, cfg)
		}
	}
	return nil
}

var assignConfig = func(c *Cluster, host, config string) error {
	cmd := FSMCommand{Type: cmdAssignConfig, Host: host, Config: config}
	if err := c.raftApply(cmd); err != nil {
		return errors.Wrapf(err, "Failed to assign config '%s' to host '%s'", config, host)
	}
	return nil
}

var releaseConfig = func(c *Cluster, host, config string) error {
	cmd := FSMCommand{Type: cmdRemoveConfig, Host: host, Config: config}
	if err := c.raftApply(cmd); err != nil {
		return errors.Wrapf(err, "Failed to release config '%s' from host '%s'", config, host)
	}
	return nil
}

func (c *FSM) handleTask(config string, stopCh chan struct{}) {
	var iteration uint64
	log := c.log.WithField("config", config)

RECLIENT:
	cl, err := NewClient()
	if err != nil {
		select {
		case <-stopCh:
			return
		default:
		}
		log.Errorf("scheduler: Can't create client %s", err)
		time.Sleep(c.updateInterval)
		goto RECLIENT
	}
	GlobalObserver.RegisterClient(cl, config)
	defer GlobalObserver.UnregisterClient(cl.ID, config)

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		iteration++
		if err = cl.Dispatch(iteration, config, genUniqueID, shouldWait); err != nil {
			log.WithField("iteration", iteration).Errorf("scheduler: Dispatch error %s", err)
			time.Sleep(c.updateInterval)
		}
	}
}
