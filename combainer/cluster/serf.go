package cluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common/configs"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

const (
	raftPool           = 5
	raftPort           = 9001
	raftTimeout        = 10 * time.Second
	retainRaftSnapshot = 2
	raftStateDirectory = "raft/"
)

// Cluster is wrapper for access cluster members
type Cluster struct {
	serfEventCh chan serf.Event
	serf        *serf.Serf
	ShutdownCh  chan struct{}

	raft       *raft.Raft
	transport  *raft.NetworkTransport
	store      *raftboltdb.BoltStore
	raftConfig *raft.Config
	m          *sync.Mutex

	log    *logrus.Entry
	config *configs.ClusterConfig
}

// Hosts return names of alive serf members
func (c *Cluster) Hosts() []string {
	all := c.serf.Members()
	alive := make([]string, 0, len(all))
	for _, m := range all {
		// that return only alive nodes
		if m.Status == serf.StatusAlive {
			alive = append(alive, m.Name)
		}
	}
	return alive
}

// Members return alive serf members
func (c *Cluster) Members() []serf.Member {
	all := c.serf.Members()
	alive := make([]serf.Member, 0, len(all))
	for _, m := range all {
		// that return only alive nodes
		if m.Status == serf.StatusAlive {
			alive = append(alive, m)
		}
	}
	return alive
}

// EventHandler is used to handle events from the serf cluster
func (c *Cluster) EventHandler() {
	for {
		select {
		case e := <-c.serfEventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				c.nodeJoin(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				c.nodeFailed(e.(serf.MemberEvent))
			case serf.EventMemberUpdate, serf.EventMemberReap,
				serf.EventUser, serf.EventQuery: // Ignore
			default:
				c.log.Warnf("unhandled event: %#v", e)
			}

		case <-c.ShutdownCh:
			return
		}
	}
}

// Bootstrap is used to attempt join to existing serf cluster.
func (c *Cluster) Bootstrap(initHosts []string) error {
	c.log.Infof("Connect to Serf cluster: %s", initHosts)
	n, err := c.serf.Join(initHosts, true)
	if n > 0 {
		c.log.Infof("Combainer joined to cluster: %d nodes", n)
	}
	if err != nil {
		c.log.Errorf("Combainer error joining to cluster: %d nodes", n)
		return err
	}
	c.transport, err = raft.NewTCPTransport(
		c.config.RaftAddr, nil, raftPool, raftTimeout, c.log.Logger.Writer(),
	)
	if err != nil {
		return errors.Wrap(err, "tcp transport failed")
	}

	var peersAddrs []string
	for _, m := range c.Members() {
		addr := net.JoinHostPort(m.Addr.String(), fmt.Sprintf("%d", c.config.RaftPort))
		peersAddrs = append(peersAddrs, addr)
	}
	raftPeers := raft.NewJSONPeers(c.config.RaftState, c.transport)
	if err = raftPeers.SetPeers(peersAddrs); err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(
		c.config.RaftState, retainRaftSnapshot, c.log.Logger.Writer(),
	)
	if err != nil {
		return err
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(c.config.RaftState, "raft.db"))
	if err != nil {
		return errors.Wrap(err, "bolt store failed")
	}
	c.store = boltStore
	c.raftConfig = raft.DefaultConfig()
	raft, err := raft.NewRaft(c.raftConfig, (*fsm)(c), boltStore, boltStore, snapshots, raftPeers, c.transport)
	if err != nil {
		c.store.Close()
		c.transport.Close()
		return errors.Wrap(err, "raft failed")
	}
	c.raft = raft

	return nil
}

// nodeJoin is used to handle join events on the serf cluster
func (c *Cluster) nodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		c.log.WithField("source", "Serf").Infof("Serf join event from %s", m.Name)
	}
}

// nodeFailed is used to handle fail events on the serf cluster
func (c *Cluster) nodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		c.log.WithField("source", "Serf").Infof("Serf failed event from %s", m.Name)
	}
}

// New create and initialize Cluster instance
func New(cfg configs.ClusterConfig) (*Cluster, error) {
	err := validateConfig(&cfg)
	if err != nil {
		return nil, err
	}

	log := logrus.WithField("source", "Cluster")
	conf := serf.DefaultConfig()
	conf.Init()
	// set tags here
	// conf.Tags[<tagname>] = <tagValue>

	eventCh := make(chan serf.Event, 256)
	conf.EventCh = eventCh

	conf.MemberlistConfig.BindAddr = cfg.BindAddr
	conf.RejoinAfterLeave = true
	conf.SnapshotPath = cfg.SnapshotPath

	conf.LogOutput = log.Logger.Writer()
	conf.MemberlistConfig.LogOutput = conf.LogOutput

	ips, err := net.LookupIP(conf.MemberlistConfig.Name)
	if err != nil || len(ips) == 0 {
		return nil, errors.Wrapf(err, "failed to LookupIP for: %s", conf.MemberlistConfig.Name)
	}
	for _, ip := range ips {
		if len(ip) == net.IPv6len && ip.IsGlobalUnicast() {
			conf.MemberlistConfig.AdvertiseAddr = ip.String()
			log.Infof("Advertise Serf address: %s", conf.MemberlistConfig.AdvertiseAddr)
			break
		}
	}

	// run Serf instance and monitor for this events
	cSerf, err := serf.Create(conf)
	if err != nil {
		if cSerf != nil {
			cSerf.Shutdown()
		}
		log.Fatalf("Failed to start serf: %s", err)
		return nil, err
	}
	c := &Cluster{
		serfEventCh: eventCh,
		ShutdownCh:  make(chan struct{}),
		serf:        cSerf,
		log:         log,
		config:      &cfg,
		m:           &sync.Mutex{},
	}
	return c, nil
}

func validateConfig(cfg *configs.ClusterConfig) error {
	if cfg.BindAddr == "" {
		cfg.BindAddr = "::"
	}
	if cfg.RaftPort == 0 {
		cfg.RaftPort = raftPort
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/spool/combainer"
	}
	cfg.RaftState = filepath.Join(cfg.DataDir, raftStateDirectory)
	if err := os.MkdirAll(cfg.RaftState, 0755); err != nil {
		return errors.Wrap(err, "failed to make data directory")
	}

	return nil
}
