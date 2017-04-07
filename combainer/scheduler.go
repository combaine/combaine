package combainer

import (
	"sort"
	"strconv"
	"time"

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

func (c *Cluster) distributeTasks(hosts []string) error {
	c.log.Debug("distributeTasks to ", hosts)
	configs, err := c.repo.ListParsingConfigs()
	if err != nil {
		return errors.Wrap(err, "Failed to list parsing config")
	}
	configSet := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		configSet[cfg] = struct{}{}
	}

	clusterSize := len(hosts)
	if clusterSize == 0 {
		c.log.Warnf("cluster is empty, there is nowhere to distribute configs")
		return nil
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
				if err := releaseConfig(c, host, cfg); err != nil {
					return err
				}
			}
		}
	}

	sort.Sort(state)
	c.log.Debugf("Current distribution: %+v", state)

	overloadedIndex := clusterSize - 1

	var (
		wantage        int
		overloadedHost = state.hosts[overloadedIndex]
	)
ALMOST_FAIR_BALANCER:
	for _, host := range state.hosts {
		wantage = state.mean - state.qty[host]
		if wantage <= 0 {
			// rebalance complete
			break ALMOST_FAIR_BALANCER
		}
		if state.remainder > 0 {
			wantage++
			state.remainder--
		}
		c.log.Debugf("Rebalance host %s (wantage %d, has %d)", host, wantage, state.qty[host])

		if len(configSet) > 0 {
			// distribute free configs
			toAdd := min(len(configSet), wantage)
			configsToAssign := make([]string, 0, toAdd)
			for cfg := range configSet {
				configsToAssign = append(configsToAssign, cfg)
				toAdd--
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
		} else {
			// rebalance assigned configs
			if state.qty[overloadedHost]-state.mean <= 1 {
				overloadedIndex--
				overloadedHost = state.hosts[overloadedIndex]
			}
			if host == overloadedHost {
				// all hosts rebalanced
				break ALMOST_FAIR_BALANCER
			}
			toRelase := min(state.qty[overloadedHost]-state.mean, wantage)
			if toRelase <= 0 {
				break
			}
			freedConfigs := make([]string, 0, toRelase)

			for _, cfg := range c.store.List(overloadedHost) {
				toRelase--
				if toRelase <= 0 {
					break
				}
				if err := releaseConfig(c, overloadedHost, cfg); err != nil {
					return err
				}
				state.qty[overloadedHost]--
				freedConfigs = append(freedConfigs, cfg)
			}
			for _, cfg := range freedConfigs {
				if err := assignConfig(c, host, cfg); err != nil {
					return err
				}
			}
		}
	}
	c.log.Debugf("Rebalanced FSM store stats %v", c.store.DistributionStatistic())
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
