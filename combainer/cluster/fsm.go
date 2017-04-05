package cluster

import (
	"encoding/json"
	"io"
	"sync"
	"time"

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
		c.store.Put(cmd.Host, cmd.Config)
		// TODO: run new client, resolve case when two or more client run in parallel
	case removeConfig:
		c.store.Remove(cmd.Host, cmd.Config)
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

// NewFSMStore create new fsm storage
func NewFSMStore() *FSMStore {
	return &FSMStore{store: make(map[string]map[string]int64)}
}

// FSMStore contains dispached congis
type FSMStore struct {
	sync.RWMutex
	store map[string]map[string]int64
}

// List return configs assigned to host
func (s *FSMStore) List(host string) []string {
	s.RLock()
	defer s.RUnlock()

	if hostConfigs, ok := s.store[host]; ok {
		configs := make([]string, 0, len(hostConfigs))
		for n := range hostConfigs {
			configs = append(configs, n)
		}
		return configs
	}
	return nil
}

// Get return unixtime, true when config added to store,
// or 0, false if configs not present
func (s *FSMStore) Get(host, config string) (int64, bool) {
	s.RLock()
	defer s.RUnlock()

	if hostConfigs, ok := s.store[host]; ok {
		if ts, ok := hostConfigs[config]; ok {
			return ts, true
		}
	}
	return 0, false
}

// Put assign new config to host
func (s *FSMStore) Put(host, config string) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.store[host]; !ok {
		s.store[host] = make(map[string]int64)
	}
	s.store[host][config] = time.Now().Unix()
}

// Remove remove config from host's store
func (s *FSMStore) Remove(host, config string) {
	s.Lock()
	defer s.Unlock()

	delete(s.store[host], config)
}
