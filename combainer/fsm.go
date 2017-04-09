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
			c.log.Errorf("Error while applying raft command: %v", r)
		}
	}()

	var cmd FSMCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		c.log.Errorf("json unmarshal: bad raft command: %v", err)
		return nil
	}
	c.log.WithField("source", "FSM").Debugf("Apply cmd %+v", cmd)
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
//func (s *FSMStore) Get(host, config string) (chan struct{}, bool) {
//	s.RLock()
//	defer s.RUnlock()
//
//	if hostConfigs, ok := s.store[host]; ok {
//		if stopCh, ok := hostConfigs[config]; ok {
//			return stopCh, true
//		}
//	}
//	return nil, false
//}

// Put assign new config to host
func (s *FSMStore) Put(host, config string) chan struct{} {
	s.Lock()
	defer s.Unlock()

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
	return newStopCh
}

// Remove remove config from host's store
func (s *FSMStore) Remove(host, config string) {
	s.Lock()
	defer s.Unlock()

	if hostConfigs, ok := s.store[host]; ok {
		if stopCh, ok := hostConfigs[config]; ok {
			if stopCh != nil {
				close(stopCh)
			}
			delete(hostConfigs, config)
		}
	}
}

// Dump FSM store keys
//func (s *FSMStore) Dump() map[string][]string {
//	s.RLock()
//	defer s.RUnlock()
//	dump := make(map[string][]string, len(s.store))
//	for k := range s.store {
//		for cfg := range s.store[k] {
//			dump[k] = append(dump[k], cfg)
//		}
//	}
//	return dump
//}

// DistributionStatistic dump number of configs assigned to hosts
func (s *FSMStore) DistributionStatistic() [][2]string {
	s.RLock()
	defer s.RUnlock()

	dump := make([][2]string, 0, len(s.store))
	for k := range s.store {
		dump = append(dump, [2]string{k, strconv.Itoa(len(s.store[k]))})
	}
	return dump
}
