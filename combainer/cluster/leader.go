package cluster

import (
	"encoding/json"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
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
	update := time.After(c.updateInterval)

RECONCILE:
	reconcileCh = nil
	interval := time.After(60 * time.Second)

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
	goto WAIT

WAIT:
	for {
		select {
		case <-stopCh:
			return
		case <-c.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case <-update:
			update = time.After(c.updateInterval)

			if err := c.distributeTasks(); err != nil {
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
		c.log.Errorf("failed to add raft peer: %v", err)
		return err
	} else if err == nil {
		c.log.Infof("added raft peer: %v", addr)
	}
	return nil

}

func (c *Cluster) removeRaftPeer(m serf.Member) error {
	addr := (&net.TCPAddr{IP: m.Addr, Port: c.config.RaftPort}).String()

	future := c.raft.RemovePeer(addr)
	if err := future.Error(); err != nil && err != raft.ErrUnknownPeer {
		c.log.Errorf("failed to remove raft peer: %v", err)
		return err
	} else if err == nil {
		c.log.Infof("removed raft peer: %v", addr)
	}
	return nil
}

func (c *Cluster) raftApply(command *fsmCommand) error {
	state, err := json.Marshal(command)
	if err != nil {
		return err
	}
	f := c.raft.Apply(state, raftTimeout)
	return f.Error()
}
