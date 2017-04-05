package cluster

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type fsm Cluster

// RaftCmdType describe storage operation
type RaftCmdType int

const (
	addConfig RaftCmdType = iota
	removeConfig
)

// fsmCommand contains cluster storage operation with data
type fsmCommand struct {
	Type   RaftCmdType `json:"type"`
	Host   string      `json:"host"`
	Config string      `json:"config"`
}

// Apply command received over raft
func (c *fsm) Apply(l *raft.Log) interface{} {
	defer func() {
		if r := recover(); r != nil {
			c.log.Errorf("Error while applying raft command: %v", r)
		}
	}()

	var cmd fsmCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		c.log.Errorf("json unmarshal: bad raft command: %v", err)
		return nil
	}
	c.log.WithField("source", "fsm").Debugf("Apply cmd %+v", cmd)
	switch cmd.Type {
	case addConfig:
		// TODO
	case removeConfig:
		// TODO
	}
	return nil
}

// Restore fsm from snapshot
func (c *fsm) Restore(rc io.ReadCloser) error {
	return nil
}

// FSMSnapshot ...
type FSMSnapshot struct{}

// Persist ...
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

// Release ...
func (f *FSMSnapshot) Release() {}

// Snapshot create fsm snapshot
func (c *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{}, nil
}
