package lockserver

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"

	"launchpad.net/gozk/zookeeper"

	"github.com/Combaine/Combaine/common/configs"
)

type locksInfo struct {
	AllLocks []string
	Version  int
}

type LockServer struct {
	log     *logrus.Entry
	mu      sync.Mutex
	Zk      *zookeeper.Conn
	Session <-chan zookeeper.Event
	stop    chan struct{}
	configs.LockServerSection
	locksInfo
}

func NewLockServer(config configs.LockServerSection) (*LockServer, error) {
	endpoints := strings.Join(config.Hosts, ",")
	log := logrus.WithField("source", "zookeeper")
	log.Infof("connecting to %s", endpoints)
	zk, session, err := zookeeper.Dial(endpoints, 5e9)
	if err != nil {
		log.Errorf("Zookeeper: unable to connect to %s %s", endpoints, err)
		return nil, err
	}

	deadline := time.After(time.Duration(config.Timeout) * time.Second)

ZK_CONNECTING_WAIT_LOOP:
	for {
		select {
		case event := <-session:
			if !event.Ok() {
				err = fmt.Errorf("%s", event.String())
				log.Errorf("connection error: %s", err)
				return nil, err
			}

			switch event.State {
			case zookeeper.STATE_CONNECTED:
				log.Infof("connected to Zookeeper successfully")
				break ZK_CONNECTING_WAIT_LOOP
			case zookeeper.STATE_CONNECTING:
				log.Infof("connecting to Zookeeper")
			default:
				log.Warningf("unexpectable Zookeeper session event: %s", event)
			}
		case <-deadline:
			zk.Close()
			log.Error("connection deadline exceeded")
			return nil, fmt.Errorf("connection deadline exceeded")
		}
	}

	ls := &LockServer{
		log:               log,
		Zk:                zk,
		Session:           session,
		stop:              make(chan struct{}),
		LockServerSection: config,
		locksInfo: locksInfo{
			AllLocks: make([]string, 10),
			Version:  -1,
		},
	}

	return ls, nil
}

func (ls *LockServer) Lock(node string) error {
	path := fmt.Sprintf("/%s/%s", ls.LockServerSection.Id, node)
	ls.log.Infof("Locking %s", path)
	content, err := os.Hostname()
	if err != nil {
		return err
	}
	if meta, _ := ls.Zk.Exists(path); meta != nil {
		return fmt.Errorf("Node %s alredy exists", path)
	}
	_, err = ls.Zk.Create(path, content, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (ls *LockServer) Unlock(node string) error {
	path := fmt.Sprintf("/%s/%s", ls.LockServerSection.Id, node)
	ls.log.Infof("Unlocking %s", path)
	return ls.Zk.Delete(path, -1)
}

func (ls *LockServer) Watch(node string) (<-chan zookeeper.Event, error) {
	path := fmt.Sprintf("/%s/%s", ls.LockServerSection.Id, node)
	_, _, w, err := ls.Zk.GetW(path)
	return w, err
}

func (ls *LockServer) Locks() []string {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.locksInfo.AllLocks
}

func (ls *LockServer) updateAllLocks(children []string, stat *zookeeper.Stat) {
	ls.mu.Lock()
	if stat.CVersion() >= ls.locksInfo.Version {
		ls.locksInfo.Version = stat.CVersion()
		ls.locksInfo.AllLocks = children
	}
	ls.mu.Unlock()
}

func (ls *LockServer) watchLocks() error {
	path := fmt.Sprintf("/%s", ls.LockServerSection.Id)
	var (
		watch    <-chan zookeeper.Event
		err      error
		children []string
		stat     *zookeeper.Stat
	)

	children, stat, watch, err = ls.Zk.ChildrenW(path)
	if err != nil {
		return err
	}

	ls.updateAllLocks(children, stat)

	go func() {
		for {
			select {
			case event := <-watch:
				if !event.Ok() {
					err = fmt.Errorf("%s", event.String())
					ls.log.Errorf("locks watcher error: %s", err)
					return
				}

				children, stat, watch, err = ls.Zk.ChildrenW(path)
				if err != nil {
					ls.log.Errorf("unable to watch locks: %s", err)
					return
				}
				ls.updateAllLocks(children, stat)

			case <-ls.stop:
				return
			}
		}
	}()
	return nil
}

func (ls *LockServer) Close() {
	close(ls.stop)
	ls.Zk.Close()
}
