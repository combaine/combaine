package server

import (
	"fmt"

	"github.com/combaine/combaine/combainer"
	"github.com/combaine/combaine/common"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

const (
	// StatusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	StatusReap = serf.MemberStatus(-1)
)

// serfEventHandler is used to handle events from the serf cluster
func (s *CombaineServer) serfEventHandler() {
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
				s.log.WithField("source", "Serf").Warnf("unhandled event: %#v", e)
			}

		case <-s.shutdownCh:
			return
		}
	}
}

// connectSerf is used to attempt join to existing serf cluster.
func (s *CombaineServer) connectSerf() error {
	f, err := combainer.LoadHostFetcher(s.GetContext(), s.CombainerConfig.CloudSection.HostFetcher)
	if err != nil {
		return err
	}
	hostsByDc, err := f.Fetch(s.CombainerConfig.MainSection.CloudGroup)
	if err != nil {
		return fmt.Errorf("Failed to fetch cloud group: %s", err)
	}

	hosts := hostsByDc.RemoteHosts()
	s.log.Infof("Connect to Serf cluster: %s", hosts)
	n, err := s.Serf.Join(hosts, true)
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
func (s *CombaineServer) nodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		s.log.WithField("source", "Serf").Infof("Join new combainer %s", m.Name)
	}
}

// nodeFailed is used to handle fail events on the serf cluster
func (s *CombaineServer) nodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		s.log.WithField("source", "Serf").Infof("Failed combainer %s", m.Name)
	}
}

// setupSerf create and initialize Serf instance
func (s *CombaineServer) setupSerf() (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init() // initialize tag map
	// set tags here
	// conf.Tags[<tagname>] = <tagValue>

	// all combainer build one cross dc cluster
	conf.MemberlistConfig = memberlist.DefaultWANConfig()

	// TODO (sakateka) move to configs
	conf.MemberlistConfig.BindPort = 7946
	conf.MemberlistConfig.LogOutput = s.log.Logger.Writer()
	conf.LogOutput = s.log.Logger.Writer()

	conf.EventCh = s.serfEventCh
	conf.RejoinAfterLeave = true

	conf.SnapshotPath = s.CombainerConfig.SerfConfig.SnapshotPath
	if conf.SnapshotPath == "" {
		conf.SnapshotPath = "/var/lib/combainer/serf.snapshot"
	}
	if err := common.EnsurePath(conf.SnapshotPath, false); err != nil {
		return nil, err
	}

	return serf.Create(conf)
}
