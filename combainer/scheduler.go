package combainer

import (
	"math/rand"
	"sort"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/pkg/errors"
)

var shouldWait = true
var clientStartDelayRange int64 = 30 // [0, n) get rand from

type balance struct {
	hosts             []string
	quantity          map[string]int
	average           int
	permissibleNumber int
}

func (b *balance) Len() int           { return len(b.hosts) }
func (b *balance) Less(i, j int) bool { return b.quantity[b.hosts[i]] < b.quantity[b.hosts[j]] }
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
	clusterSize := len(hosts)
	if clusterSize == 0 {
		c.log.Warnf("scheduler: cluster is empty, there is nowhere to distribute configs")
		return nil
	}

	configs, err := repository.ListParsingConfigs()
	c.log.Debugf("scheduler: Distribute %d configs to %+v", len(configs), hosts)
	if err != nil {
		return errors.Wrap(err, "Failed to list parsing config")
	}
	configSet := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		configSet[cfg] = struct{}{}
	}

	oldStat := c.store.DistributionStatistic()
	deadNodes, oldStat := markDeadNodes(hosts, oldStat)
	c.log.Debugf("scheduler: Current FSM store stats %v, total %d, dead: %v", oldStat, len(configSet), deadNodes)

	// Release configs from deadNodes
	for _, host := range deadNodes {
		for _, cfg := range c.store.List(host) {
			if err := releaseConfig(c, host, cfg); err != nil {
				return err
			}
		}
	}

	state := &balance{
		hosts:             hosts,
		quantity:          make(map[string]int, clusterSize),
		average:           len(configs) / clusterSize,
		permissibleNumber: len(configs) / clusterSize,
	}
	if len(configs)%clusterSize > 0 {
		state.permissibleNumber++
	}

	freeConfigSet := configSet // configSet  moved to freeConfigSet
	for _, host := range state.hosts {
		for _, cfg := range c.store.List(host) {
			if _, ok := freeConfigSet[cfg]; ok {
				state.quantity[host]++
				// remove assigned configs
				delete(freeConfigSet, cfg)
			} else {
				c.log.Infof("scheduler: Release missing config %s", cfg)
				if err := releaseConfig(c, host, cfg); err != nil {
					return err
				}
			}
		}
	}

	// now overloadedHosts at end of the hosts lists
	sort.Sort(state)

	if err := c.runBalancer(state, freeConfigSet, clusterSize-1); err != nil {
		return errors.Wrap(err, "Balancer error")
	}

	curStat := c.store.DistributionStatistic()
	_, curStat = markDeadNodes(hosts, curStat)
	c.log.Infof("scheduler: Rebalanced FSM store stats %v", curStat)
	return nil
}

func (c *Cluster) runBalancer(state *balance, configSet map[string]struct{}, overloadedIndex int) error {
	var overloadedHost = state.hosts[overloadedIndex]
	for _, host := range state.hosts {
		wantage := state.average - state.quantity[host]

		setLen := len(configSet)
		if wantage == 0 && setLen > 0 {
			wantage = setLen / len(state.hosts)
		}
		// rebalance assigned configs
		if state.quantity[overloadedHost]-state.permissibleNumber <= 0 {
			if overloadedIndex >= 0 {
				overloadedHost = state.hosts[overloadedIndex]
				overloadedIndex--
			}
		}
		toRelase := min(state.quantity[overloadedHost]-state.permissibleNumber, wantage)
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
				state.quantity[overloadedHost]--
			}
		}

		toAdd := min(len(configSet), wantage)
		if toAdd > 0 {
			c.log.Infof("scheduler: Rebalance host %s (add %d, has %d, total %d)", host, toAdd, state.quantity[host], setLen)
		}
		var configsToAssign []string
		for cfg := range configSet {
			toAdd--
			configsToAssign = append(configsToAssign, cfg)
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
	clientStartDelay := time.Duration(rand.Int63n(clientStartDelayRange)+1)*time.Second + c.config.RaftUpdateInterval
	log.Infof("scheduler.handleTask: enter, clientStartDelay=%s", clientStartDelay)
	time.Sleep(clientStartDelay)
	defer func() { log.Info("scheduler.handleTask: exit") }()

RECLIENT:
	select {
	case <-stopCh:
		return
	default:
	}
	cl, err := NewClient()
	if err != nil {
		log.Errorf("scheduler: Can't create client %s, wait %s", err, c.config.RaftUpdateInterval)
		time.Sleep(c.config.RaftUpdateInterval)
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
		id := common.GenerateSessionID()
		if err = cl.Dispatch(iteration, config, id, shouldWait); err != nil {
			log.Errorf("scheduler: Dispatch error %s, iteration: %d, session: %s", err, iteration, id)
			time.Sleep(c.config.RaftUpdateInterval)
		}
	}
}
