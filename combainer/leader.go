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
				c.log.Info("cluster leadership acquired")
			} else if stopCh != nil {
				close(stopCh)
				stopCh = nil
				c.log.Info("cluster leadership lost")
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
		c.log.WithField("source", "Raft").Errorf("failed to wait for barrier: %v", err)
		goto WAIT
	}

	if err := c.reconcile(); err != nil {
		c.log.WithField("source", "Raft").Errorf("failed to reconcile: %v", err)
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
			if err := c.distributeTasks(c.Hosts()); err != nil {
				// updateTicker.Stop() // TODO if distributeTasks return error we lost leadership?
				c.log.WithField("source", "Raft").Errorf("failed to distributeTasks: %v", err)
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
		c.log.Info("failed to reconcile member: %v: %v", member, err)
		return err
	}
	return nil
}

func (c *Cluster) addRaftPeer(m serf.Member) error {
	addr := (&net.TCPAddr{IP: m.Addr, Port: c.config.RaftPort}).String()

	future := c.raft.AddPeer(addr)
	if err := future.Error(); err != nil && err != raft.ErrKnownPeer {
		c.log.Errorf("Failed to add raft peer: %v", err)
		return err
	} else if err == nil {
		c.log.Infof("Added raft peer: %+v", m)
	}
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

	future := c.raft.RemovePeer(addr)
	if err := future.Error(); err != nil && err != raft.ErrUnknownPeer {
		c.log.Errorf("Failed to remove raft peer: %v", err)
		return err
	} else if err == nil {
		c.log.Infof("Removed raft peer: %+v", m)
	}
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
