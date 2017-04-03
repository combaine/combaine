package cluster

import (
	"fmt"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common/configs"
	"github.com/hashicorp/serf/serf"
)

// Serf is wrapper for access cluster members
type Serf struct {
	serfEventCh chan serf.Event
	serf        *serf.Serf
	ShutdownCh  chan struct{}
	log         *logrus.Entry
}

// Members return alive serf members
func (s *Serf) Members() []string {
	members := s.serf.Members()
	hosts := make([]string, 0, len(members))
	for _, m := range members {
		// that return only alive nodes
		if m.Status == serf.StatusAlive {
			hosts = append(hosts, m.Name)
		}
	}
	return hosts
}

// EventHandler is used to handle events from the serf cluster
func (s *Serf) EventHandler() {
	for {
		select {
		case e := <-s.serfEventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				s.nodeJoin(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				s.nodeFailed(e.(serf.MemberEvent))
			case serf.EventMemberUpdate, serf.EventMemberReap,
				serf.EventUser, serf.EventQuery: // Ignore
			default:
				s.log.Warnf("unhandled event: %#v", e)
			}

		case <-s.ShutdownCh:
			return
		}
	}
}

// Connect is used to attempt join to existing serf cluster.
func (s *Serf) Connect(initHosts []string) error {
	s.log.Infof("Connect to Serf cluster: %s", initHosts)
	n, err := s.serf.Join(initHosts, true)
	if n > 0 {
		s.log.Infof("Combainer joined to Serf cluster: %d nodes", n)
	}
	if err != nil {
		s.log.Errorf("Combainer error joining to Serf cluster: %d nodes", n)
		return err
	}
	return nil
}

// nodeJoin is used to handle join events on the serf cluster
func (s *Serf) nodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		logrus.WithField("source", "Serf").Infof("Serf join event from %s", m.Name)
	}
}

// nodeFailed is used to handle fail events on the serf cluster
func (s *Serf) nodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		s.log.Infof("Serf failed event from %s", m.Name)
	}
}

// New create and initialize Serf instance
func New(cfg configs.SerfConfig) (*Serf, error) {
	var err error
	log := logrus.WithField("source", "Serf")
	conf := serf.DefaultConfig()
	conf.Init()
	// set tags here
	// conf.Tags[<tagname>] = <tagValue>

	if cfg.BindAddr == "" {
		cfg.BindAddr = "::"
	}

	eventCh := make(chan serf.Event, 256)

	conf.EventCh = eventCh
	conf.MemberlistConfig.BindAddr = cfg.BindAddr
	conf.RejoinAfterLeave = true
	conf.SnapshotPath = cfg.SnapshotPath

	conf.LogOutput = log.Logger.Writer()
	conf.MemberlistConfig.LogOutput = conf.LogOutput

	ips, err := net.LookupIP(conf.MemberlistConfig.Name)
	if err != nil || len(ips) == 0 {
		return nil, fmt.Errorf("failed to LookupIP for: %s", conf.MemberlistConfig.Name)
	}
	for _, ip := range ips {
		if len(ip) == net.IPv6len && ip.IsGlobalUnicast() {
			conf.MemberlistConfig.AdvertiseAddr = ip.String()
			log.Infof("Advertise Serf address: %s", conf.MemberlistConfig.AdvertiseAddr)
			break
		}
	}

	// run Serf instance and monitor for this events
	serfInstance, err := serf.Create(conf)
	if err != nil {
		if serfInstance != nil {
			serfInstance.Shutdown()
		}
		log.Fatalf("Failed to start serf: %s", err)
		return nil, err
	}
	s := &Serf{
		serfEventCh: eventCh,
		ShutdownCh:  make(chan struct{}),
		serf:        serfInstance,
		log:         log,
	}
	return s, nil
}
