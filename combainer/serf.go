package combainer

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	raftPool           = 5
	raftPort           = 9001
	raftTimeout        = 10 * time.Second
	retainRaftSnapshot = 2
	raftStateDirectory = "raft/"

	// statusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	statusReap = serf.MemberStatus(-1)
)

// NewCluster create and initialize Cluster instance
func NewCluster(repo common.Repository, cfg common.ClusterConfig) (*Cluster, error) {
	err := validateConfig(&cfg)
	if err != nil {
		return nil, err
	}

	log := logrus.WithField("source", "cluster")
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
	var raftAdvertiseIP net.IP
	for _, ip := range ips {
		if ip.IsGlobalUnicast() {
			raftAdvertiseIP = ip
			conf.MemberlistConfig.AdvertiseAddr = ip.String()
			log.Infof("Advertise Serf address: %s", conf.MemberlistConfig.AdvertiseAddr)
			break
		}
	}
	if conf.MemberlistConfig.AdvertiseAddr == "" {
		return nil, errors.New("AdvertiseAddr is not set for Serf")
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
		Name:        conf.MemberlistConfig.Name,
		eventCh:     eventCh,
		serf:        cSerf,
		reconcileCh: make(chan serf.Member, 32),

		shutdownCh: make(chan struct{}),
		leaderCh:   make(chan bool, 1),

		raftAdvertiseIP: raftAdvertiseIP,
		store:           NewFSMStore(),
		log:             log,
		repo:            repo,
		config:          &cfg,
	}
	return c, nil
}

// Cluster is wrapper for access cluster members
type Cluster struct {
	Name string

	// eventCh is used to receive events from the serf cluster
	eventCh chan serf.Event
	serf    *serf.Serf

	// reconcileCh is used to pass events from the serf handler
	// into the leader manager. Mostly used to handle when servers join/leave.
	reconcileCh chan serf.Member

	shutdownCh chan struct{}
	leaderCh   chan bool

	raftAdvertiseIP net.IP
	raft            *raft.Raft
	transport       *raft.NetworkTransport
	raftStore       *raftboltdb.BoltStore
	raftConfig      *raft.Config

	store          *FSMStore
	updateInterval time.Duration
	log            *logrus.Entry
	repo           common.Repository
	config         *common.ClusterConfig
}

// Bootstrap is used to attempt join to existing serf cluster.
// and bootstrap the Raft agent using cluster as FSM.
// Updates leadership are returned on leaderCh,
// leader dispatch new configs every interval time.
func (c *Cluster) Bootstrap(initHosts []string, interval time.Duration) error {
	c.log.Infof("Bootstrap cluster, connect Serf nodes: %s", initHosts)
CONNECT:
	n, err := c.serf.Join(initHosts, true)
	if n > 0 {
		c.log.Infof("Combainer joined to cluster: %d nodes", n)
	}
	// NOTE: doc from serf.Join
	// Join joins an existing Serf cluster. Returns the number of nodes
	// successfully contacted. The returned error will be non-nil only in the
	// case that no nodes could be contacted.
	if err != nil {
		c.log.Errorf("Combainer error joining to cluster: %d nodes", n)
		time.Sleep(interval)
		goto CONNECT
	}

	c.log.Info("Create raft transport")
	c.transport, err = raft.NewTCPTransport(
		net.JoinHostPort(c.config.BindAddr, strconv.Itoa(c.config.RaftPort)),
		&net.TCPAddr{IP: c.raftAdvertiseIP, Port: c.config.RaftPort},
		raftPool,
		raftTimeout,
		c.log.Logger.Writer(),
	)
	if err != nil {
		return errors.Wrap(err, "tcp transport failed")
	}

	var addrs []string
	for _, m := range c.Members() {
		addr := &net.TCPAddr{IP: m.Addr, Port: c.config.RaftPort}
		addrs = append(addrs, addr.String())
	}

	c.log.Info("Create snapshot store")
	snapshots, err := raft.NewFileSnapshotStore(
		c.config.RaftStateDir, retainRaftSnapshot, c.log.Logger.Writer(),
	)
	if err != nil {
		return err
	}

	c.log.Info("Create raft log store")
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(c.config.RaftStateDir, "raft.db"))
	if err != nil {
		return errors.Wrap(err, "bolt store failed")
	}

	// Attempt a live bootstrap
	var configuration raft.Configuration
	for _, addr := range addrs {
		server := raft.Server{
			ID:      raft.ServerID(addr),
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, server)
	}
	c.log.Infof("Attempting to bootstrap cluster with peers (%s)", strings.Join(addrs, ","))
	future := c.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		c.log.Errorf("Failed to bootstrap cluster: %v", err)
	}

	c.raftStore = boltStore
	c.raftConfig = raft.DefaultConfig()
	c.raftConfig.NotifyCh = c.leaderCh
	c.raftConfig.LogOutput = c.log.Logger.Writer()
	c.raftConfig.StartAsLeader = c.config.StartAsLeader
	c.updateInterval = interval

	c.log.Info("Create raft")
	raft, err := raft.NewRaft(c.raftConfig, (*FSM)(c), boltStore, boltStore, snapshots, c.transport)
	if err != nil {
		return errors.Wrap(err, "raft failed")
	}
	c.raft = raft

	return nil
}

// Hosts return names of alive serf members
func (c *Cluster) Hosts() []string {
	if c.serf == nil {
		return nil
	}
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
		case e := <-c.eventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				c.nodeJoin(e.(serf.MemberEvent))
				c.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				c.nodeFailed(e.(serf.MemberEvent))
				c.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberReap:
				c.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberUpdate, serf.EventUser, serf.EventQuery: // Ignore
			default:
				c.log.Warnf("unhandled serf event: %#v", e)
			}

		case <-c.shutdownCh:
			return
		}
	}
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

// localMemberEvent is used to reconcile Serf events with the
// consistent store if we are the current leader.
func (c *Cluster) localMemberEvent(me serf.MemberEvent) {
	// Do nothing if we are not the leader
	if !c.IsLeader() {
		return
	}

	// Check if this is a reap event
	isReap := me.EventType() == serf.EventMemberReap

	// Queue the members for reconciliation
	for _, m := range me.Members {
		// Change the status if this is a reap event
		if isReap {
			m.Status = statusReap
		}
		select {
		case c.reconcileCh <- m:
		default:
		}
	}
}

func validateConfig(cfg *common.ClusterConfig) error {
	if cfg.BindAddr == "" {
		cfg.BindAddr = "::"
	}
	if cfg.RaftPort == 0 {
		cfg.RaftPort = raftPort
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/spool/combainer"
	}
	cfg.RaftStateDir = filepath.Join(cfg.DataDir, raftStateDirectory)
	if err := os.MkdirAll(cfg.RaftStateDir, 0755); err != nil {
		return errors.Wrap(err, "failed to make data directory")
	}

	return nil
}

// Shutdown try gracefully shutdown raft cluster
func (c *Cluster) Shutdown() {
	c.log.Info("Shutdown cluster")
	if c.shutdownCh != nil {
		close(c.shutdownCh)
		c.shutdownCh = nil
	}
	if c.raft != nil {
		if err := c.raft.Shutdown().Error(); err != nil {
			c.log.Errorf("failed to shutdown raft: %s", err)
		}
	}
	if c.transport != nil {
		if err := c.transport.Close(); err != nil {
			c.log.Errorf("failed to close raft transport %v", err)
		}
	}
	if c.raftStore != nil {
		if err := c.raftStore.Close(); err != nil {
			c.log.Errorf("failed to close raftStore: %s", err)
		}
	}
}
