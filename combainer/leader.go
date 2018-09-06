package combainer

import (
	"encoding/json"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

// Run is used to monitor if we acquire or lose our role as the
// leader in the Raft cluster.
// If we are leader then we distribute tasks over cluster
func (c *Cluster) Run() {
	var stopCh chan struct{}
	for {
		select {
		case isLeader := <-c.leaderCh:
			if isLeader {
				stopCh = make(chan struct{})
				go c.leaderLoop(stopCh)
				c.log.Info("leader: cluster leadership acquired")
			} else if stopCh != nil {
				close(stopCh)
				stopCh = nil
				c.log.Info("leader: cluster leadership lost")
			}
		case <-c.shutdownCh:
			return
		}
	}
}

// leaderLoop runs as long as we are the leader to run maintainence duties
func (c *Cluster) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member

	updateTicker := time.NewTicker(c.updateInterval)
	reconcileTicker := time.NewTicker(60 * time.Second)
	defer func() {
		updateTicker.Stop()
		reconcileTicker.Stop()
	}()

RECONCILE:
	reconcileCh = nil

	barrier := c.raft.Barrier(0)
	if err := barrier.Error(); err != nil {
		c.log.Errorf("loeader: failed to wait for barrier: %v", err)
		goto WAIT
	}

	if err := c.reconcile(); err != nil {
		c.log.Errorf("leader: failed to reconcile: %v", err)
	} else {
		reconcileCh = c.reconcileCh
	}

WAIT:
	for {
		select {
		case <-stopCh:
			return
		case <-c.shutdownCh:
			return
		case <-reconcileTicker.C:
			goto RECONCILE
		case <-updateTicker.C:
			hosts, err := c.Peers()
			if err != nil {
				c.log.Errorf("leader: failed to get raft peers: %v", err)
				// return // TODO if perrs return error we lost leadership?
				// but eventually loss of leadership will break this loop
			}
			if err := c.distributeTasks(hosts); err != nil {
				c.log.Errorf("leader: failed to distributeTasks: %v", err)
			}
		case member := <-reconcileCh:
			if c.IsLeader() {
				c.reconcileMember(member)
			}
		}
	}
}

// IsLeader checks if this server is the cluster leader
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// reconcile is used to reconcile the differences between Serf
// membership and what is reflected in our strongly consistent store.
func (c *Cluster) reconcile() error {
	members := c.serf.Members()
	for _, member := range members {
		if err := c.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

// reconcileMember is used to do an async reconcile of a single serf member
func (c *Cluster) reconcileMember(member serf.Member) error {
	// Do not reconcile ourself
	if member.Name == c.Name {
		return nil
	}

	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = c.addRaftPeer(member)
	case serf.StatusLeft, statusReap:
		err = c.removeRaftPeer(member)
	}
	if err != nil {
		c.log.Errorf("leader: Failed to reconcile member: %v: %v", member, err)
		return err
	}
	return nil
}

func (c *Cluster) addRaftPeer(m serf.Member) error {
	addr := (&net.TCPAddr{IP: m.Addr, Port: c.config.RaftPort}).String()

	// See if it's already in the configuration. It's harmless to re-add it
	// but we want to avoid doing that if possible to prevent useless Raft
	// log entries.
	configFuture := c.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		c.log.Errorf("leader: Failed to get raft configuration: %v", err)
		return err
	}
	for _, server := range configFuture.Configuration().Servers {
		if server.Address == raft.ServerAddress(addr) {
			return nil
		}
	}
	// add as a peer
	future := c.raft.AddVoter(raft.ServerID(m.Name), raft.ServerAddress(addr), 0, time.Minute)
	if err := future.Error(); err != nil {
		c.log.Errorf("leader: Failed to add raft peer: %v", err)
		return err
	}
	c.log.Infof("leader: Added raft peer: %+v", m)
	return nil
}

func (c *Cluster) removeRaftPeer(m serf.Member) error {
	for _, cfg := range c.store.List(m.Name) {
		cmd := FSMCommand{Type: cmdRemoveConfig, Host: m.Name, Config: cfg}
		if err := c.raftApply(cmd); err != nil {
			return errors.Wrapf(err, "Failed to remove config '%s' from host '%s'", cfg, m.Name)
		}
	}

	addr := (&net.TCPAddr{IP: m.Addr, Port: c.config.RaftPort}).String()

	// See if it's already in the configuration. It's harmless to re-remove it
	// but we want to avoid doing that if possible to prevent useless Raft
	// log entries.
	configFuture := c.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		c.log.Errorf("leader: Failed to get raft configuration: %v", err)
		return err
	}
	for _, server := range configFuture.Configuration().Servers {
		if server.Address == raft.ServerAddress(addr) {
			goto REMOVE
		}
	}
	return nil

REMOVE:
	// remove as a peer
	future := c.raft.RemoveServer(raft.ServerID(m.Name), 0, 0)
	if err := future.Error(); err != nil {
		c.log.Errorf("leader: Failed to remove raft peer '%s': %v", addr, err)
		return err
	}
	c.log.Infof("leader: Removed raft peer: %+v", m)
	return nil
}

func (c *Cluster) raftApply(command FSMCommand) error {
	state, err := json.Marshal(command)
	if err != nil {
		return err
	}
	f := c.raft.Apply(state, raftTimeout)
	return f.Error()
}
