package combainer

import (
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	raftPool    = 5
	raftPort    = 9001
	raftTimeout = 10 * time.Second

	// statusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	statusReap   = serf.MemberStatus(-1)
	leaderTagKey = "leader"
)

// NewCluster create and initialize Cluster instance
func NewCluster(cfg repository.ClusterConfig) (*Cluster, error) {
	err := validateConfig(&cfg)
	if err != nil {
		return nil, errors.Wrap(err, "validateConfig")
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

	conf.LogOutput = log.Logger.Writer()
	conf.MemberlistConfig.LogOutput = conf.LogOutput

	ips, err := net.LookupIP(conf.MemberlistConfig.Name) // is set to hostname in serf.DefaultConfig()
	if err != nil || len(ips) == 0 {
		return nil, errors.Wrapf(err, "failed to LookupIP for: %s", conf.MemberlistConfig.Name)
	}
	var raftAdvertiseIP net.IP
	for _, ip := range ips {
		if ip.IsGlobalUnicast() && ip.To4() == nil {
			raftAdvertiseIP = ip
			conf.MemberlistConfig.AdvertiseAddr = ip.String()
			log.Infof("Advertise Memberlist address: %s", conf.MemberlistConfig.AdvertiseAddr)
			break
		}
	}
	if conf.MemberlistConfig.AdvertiseAddr == "" {
		return nil, errors.New("AdvertiseAddr is not set for Memberlist")
	}

	// run Serf instance and monitor for this events
	cSerf, err := serf.Create(conf)
	if err != nil {
		log.Fatalf("Failed to start serf: %s", err)
		if cSerf != nil {
			cSerf.Shutdown()
		}
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
		config:          &cfg,
	}
	GenerateAndRegisterSerfResolver(c.AliveMembers)

	log.Info("create raft transport")
	if err := c.createRaftTransport(); err != nil {
		return nil, err
	}

	// handle serf events
	go c.EventHandler()

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
	raftConfig      *raft.Config
	raftStore       *raft.InmemStore
	raftSnapStore   *raft.InmemSnapshotStore

	store  *FSMStore
	log    *logrus.Entry
	config *repository.ClusterConfig
	once   sync.Once
}

// join this not to serf cluster
func (c *Cluster) joinSerf(initHosts []string) {
	c.log.Infof("joinSerf: entrance to the connect loop, nodes: %s", initHosts)
CONNECT:
	n, err := c.serf.Join(initHosts, true)
	if n > 0 {
		c.log.Infof("joinSerf: Combainer joined to cluster: %d nodes", n)
	}
	// NOTE: doc from serf.Join
	// Join joins an existing Serf cluster. Returns the number of nodes
	// successfully contacted. The returned error will be non-nil only in the
	// case that no nodes could be contacted.
	if err != nil {
		c.log.Errorf("joinSerf: Combainer error joining to cluster: %s", err)
		time.Sleep(c.config.RaftUpdateInterval)
		goto CONNECT
	}
	return
}

func (c *Cluster) createRaftTransport() error {
	trans, err := raft.NewTCPTransport(
		net.JoinHostPort(c.config.BindAddr, strconv.Itoa(c.config.RaftPort)),
		&net.TCPAddr{IP: c.raftAdvertiseIP, Port: c.config.RaftPort},
		raftPool,
		raftTimeout,
		c.log.Logger.Writer(),
	)
	if err != nil {
		return errors.Wrap(err, "tcp transport failed")
	}
	c.transport = trans
	return nil
}

// configure raft stores and transport
func (c *Cluster) setupRaft() {
	c.log.Info("setupRaft: initialize store")
	c.raftStore = raft.NewInmemStore()
	c.raftSnapStore = raft.NewInmemSnapshotStore()

	c.log.Info("setupRaft: initialize raft config")
	c.raftConfig = raft.DefaultConfig()
	c.raftConfig.NotifyCh = c.leaderCh
	c.raftConfig.LogOutput = c.log.Logger.Writer()
	c.raftConfig.StartAsLeader = c.config.StartAsLeader
	c.raftConfig.LocalID = raft.ServerID(utils.Hostname())
}

// attempt to bootstrap raft cluster
func (c *Cluster) maybeBootstrap() error {
	serfMembers := c.AliveMembers()
	if len(serfMembers) < int(c.config.BootstrapExpect) {
		return nil
	}
	c.once.Do(c.setupRaft)

	c.log.Infof("bootstrap: Attempting to bootstrap cluster")
	var servers []raft.Server
	for _, m := range serfMembers {
		servers = append(servers, raft.Server{
			ID: raft.ServerID(m.Name),
			Address: raft.ServerAddress(
				net.JoinHostPort(m.Addr.String(), strconv.Itoa(c.config.RaftPort)),
			),
		})
	}
	var configuration raft.Configuration
	configuration.Servers = servers

	if err := raft.BootstrapCluster(
		c.raftConfig,
		c.raftStore, c.raftStore, c.raftSnapStore,
		c.transport, configuration); err != nil {
		return errors.Wrap(err, "raft.BootstrapCluster")
	}

	c.log.Info("bootstrap: Create raft")
	raft, err := raft.NewRaft(
		c.raftConfig, (*FSM)(c),
		c.raftStore, c.raftStore, c.raftSnapStore,
		c.transport,
	)
	if err != nil {
		return errors.Wrap(err, "raft.NewRaft")
	}
	c.raft = raft
	// reset BootstrapExpect
	c.config.BootstrapExpect = 0

	return nil
}

// Peers is used to return alive and valid raft peers.
func (c *Cluster) Peers() ([]string, error) {
	if c.raft == nil {
		return nil, errors.New("Peers: cluster raft not configured")
	}
	future := c.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	config := future.Configuration()

	aliveHosts := make(map[string]struct{})
	for _, sm := range c.AliveMembers() {
		if l, ok := sm.Tags[leaderTagKey]; ok && l != "" {
			// only members who see a leader
			aliveHosts[sm.Name] = struct{}{}
		}
	}

	var peers []string
	for _, server := range config.Servers {
		peersID := string(server.ID)
		if _, ok := aliveHosts[peersID]; ok {
			peers = append(peers, peersID)
		}
	}
	return peers, nil
}

// Hosts return names of alive serf members
func (c *Cluster) Hosts() []string {
	members := c.AliveMembers()
	hosts := make([]string, len(members))
	for i, m := range members {
		hosts[i] = m.Name
	}
	return hosts
}

// AliveMembers return alive serf members
func (c *Cluster) AliveMembers() []serf.Member {
	if c.serf == nil {
		return nil
	}
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
		if c.config.BootstrapExpect != 0 {
			if err := c.maybeBootstrap(); err != nil {
				c.log.WithField("source", "Serf").Infof("Failed to bootstra %s", err)
			}
		}
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

func validateConfig(cfg *repository.ClusterConfig) error {
	if cfg.BindAddr == "" {
		cfg.BindAddr = "::"
	}
	if cfg.RaftPort == 0 {
		cfg.RaftPort = raftPort
	}
	if cfg.BootstrapExpect == 0 {
		cfg.BootstrapExpect = 1
	}
	if cfg.RaftUpdateInterval < 5*time.Second {
		logrus.Errorf("validateConfig: reset RaftUpdateInterval from %s to 5 seconds", cfg.RaftUpdateInterval)
		cfg.RaftUpdateInterval = 5 * time.Second
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
}
