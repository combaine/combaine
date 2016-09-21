package server

import (
	"github.com/combaine/combaine/common"
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

// nodeJoin is used to handle join events on the serf cluster
func (s *CombaineServer) nodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		s.log.WithField("source", "Serf").Infof("Join new combainer %s", m.Name)
	}
}

// nodeFailed is used to handle fail events on the serf cluster
func (s *Server) nodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		s.log.WithField("source", "Serf").Infof("Failed combainer %s", m.Name)
	}
}

// setupSerf create and initialize Serf instance
func (s *CombaineServer) setupSerf() (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	// all combainer build one cross dc cluster
	conf.MemberlistConfig = memberlist.DefaultWANConfig()
	// TODO (sakateka) move in configs
	conf.MemberlistConfig.BindPort = "7946"

	conf.Init() // initialize tag map
	// set tags here
	// conf.Tags[<tagname>] = <tagValue>

	conf.MemberlistConfig.LogOutput = s.log
	conf.LogOutput = s.log
	conf.EventCh = ch
	conf.SnapshotPath = s.Serf.SnapshotPath
	if conf.SnapshotPath == "" {
		conf.SnapshotPath = "/var/lib/combainer/serf.snapshot"
	}
	if err := common.EnsurePath(conf.SnapshotPath, false); err != nil {
		return nil, err
	}
	conf.RejoinAfterLeave = true
	conf.Merge = &serfMergeDelegate{}

	return serf.Create(conf)
}
