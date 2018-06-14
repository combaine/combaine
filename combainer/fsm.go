package combainer

import (
	"encoding/json"
	"io"
	"strconv"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM is cluster state
type FSM Cluster

const (
	cmdAssignConfig = "AssignConfig"
	cmdRemoveConfig = "RemoveConfig"
)

// FSMCommand contains cluster storage operation with data
type FSMCommand struct {
	Type   string `json:"type"`
	Host   string `json:"host"`
	Config string `json:"config"`
}

// Apply command received over raft
func (c *FSM) Apply(l *raft.Log) interface{} {
	defer func() {
		if r := recover(); r != nil {
			c.log.Errorf("fsm: Error while applying raft command: %v", r)
		}
	}()

	var cmd FSMCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		c.log.Errorf("fsm: json unmarshal: bad raft command: %v", err)
		return nil
	}
	c.log.Infof("fsm: Apply cmd %+v", cmd)
	switch cmd.Type {
	case cmdAssignConfig:
		stopCh := c.store.Put(cmd.Host, cmd.Config)
		if cmd.Host == c.Name {
			go c.handleTask(cmd.Config, stopCh)
		}
	case cmdRemoveConfig:
		c.store.Remove(cmd.Host, cmd.Config)
	}
	return nil
}

// Restore FSM from snapshot
func (c *FSM) Restore(rc io.ReadCloser) error {
	c.log.Infof("fsm: Restore from %+v", rc)
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

// Snapshot create FSM snapshot
func (c *FSM) Snapshot() (raft.FSMSnapshot, error) {
	c.log.Info("fsm: Make snapshot")
	return &FSMSnapshot{}, nil
}

// NewFSMStore create new FSM storage
func NewFSMStore() *FSMStore {
	return &FSMStore{store: make(map[string]map[string]chan struct{})}
}

// FSMStore contains dispached congis
type FSMStore struct {
	sync.RWMutex
	store map[string]map[string]chan struct{}
}

// List return configs assigned to host
func (s *FSMStore) List(host string) []string {
	var configs []string
	s.RLock()
	if hostConfigs, ok := s.store[host]; ok {
		configs = make([]string, len(hostConfigs))
		idx := 0
		for n := range hostConfigs {
			configs[idx] = n
			idx++
		}
	}
	s.RUnlock()
	return configs
}

// Put assign new config to host
func (s *FSMStore) Put(host, config string) chan struct{} {
	s.Lock()
	if _, ok := s.store[host]; !ok {
		s.store[host] = make(map[string]chan struct{})
	} else {
		// stop previously runned clients
		if oldStopCh := s.store[host][config]; oldStopCh != nil {
			close(oldStopCh)
		}
	}
	newStopCh := make(chan struct{})
	s.store[host][config] = newStopCh
	s.Unlock()
	return newStopCh
}

// Remove remove config from host's store
func (s *FSMStore) Remove(host, config string) {
	s.Lock()
	if hostConfigs, ok := s.store[host]; ok {
		if stopCh, ok := hostConfigs[config]; ok {
			if stopCh != nil {
				close(stopCh)
			}
			delete(hostConfigs, config)
		}
	}
	s.Unlock()
}

// DistributionStatistic dump number of configs assigned to hosts
func (s *FSMStore) DistributionStatistic() [][2]string {
	idx := 0
	s.RLock()
	dump := make([][2]string, len(s.store))
	for k := range s.store {
		dump[idx] = [2]string{k, strconv.Itoa(len(s.store[k]))}
		idx++
	}
	s.RUnlock()
	return dump
}

// Replace store for testing
func (s *FSMStore) Replace(newStore map[string]map[string]chan struct{}) {
	s.Lock()
	for k := range s.store {
		for cfg := range s.store[k] {
			if ch := s.store[k][cfg]; ch != nil {
				close(ch)
			}
			delete(s.store[k], cfg)
		}
		delete(s.store, k)
	}
	for k := range newStore {
		for cfg := range newStore[k] {
			if _, ok := s.store[k]; !ok {
				s.store[k] = make(map[string]chan struct{})
			}
			s.store[k][cfg] = make(chan struct{})
		}
	}
	s.Unlock()
}
