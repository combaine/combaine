package cluster

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type fsm Cluster

// RaftCommandType describe storage operation
type RaftCommandType int

const (
	addConfig RaftCommandType = iota
	deleteConfig
)

// fsmCommand contains cluster storage operation with data
type fsmCommand struct {
	Cmd  RaftCommandType  `json:"type"`
	Data *json.RawMessage `json:"data"`
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

// Apply change fsm state
func (c *fsm) Apply(l *raft.Log) interface{} {
	return nil
}
