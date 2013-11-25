package combainer

import (
	"errors"
	"launchpad.net/gozk/zookeeper"
	"log"
	"time"
)

const DUMMY_DATA = "0"

type LockServer struct {
	Zk      *zookeeper.Conn
	session <-chan zookeeper.Event
	stop    chan bool
}

func NewLockServer(endpoints string) (*LockServer, error) {
	zk, session, err := zookeeper.Dial(endpoints, 5e9)
	if err != nil {
		log.Println("Can't connect: %v", err)
		return nil, err
	}

	select {
	case event := <-session:
		log.Println(event)
	case <-time.After(time.Second * 5):
		return nil, errors.New("Connection timeout")
	}
	return &LockServer{zk, session, make(chan bool)}, nil
}

func (ls *LockServer) AcquireLock(node string) chan bool {
	log.Println("Creating ", node)
	// Add hostname and pid into DUMMY_DATA
	path, err := ls.Zk.Create(node, DUMMY_DATA, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		log.Println(err)
		return nil
	} else {
		notify := make(chan bool)
		log.Println("Create", path)
		go ls.poller(path, notify)
		return notify
	}
}

func (ls *LockServer) Close() {
	close(ls.stop)
	ls.Zk.Close()
}

func (ls *LockServer) poller(path string, notify chan bool) {
	getWatcher := func(path string) (<-chan zookeeper.Event, error) {
		_, _, watcher, err := ls.Zk.GetW(path)
		if err != nil {
			return nil, err
		}
		return watcher, nil
	}
	var watcher <-chan zookeeper.Event
	watcher, _ = getWatcher(path)
	for {
		select {
		case event := <-ls.session:
			log.Println(event)
		case <-ls.stop:
			log.Println("Stop the poller")
			return
		case event := <-watcher:
			if !event.Ok() || event.Type == zookeeper.EVENT_DELETED {
				log.Println("Stop poller. Error.")
				notify <- false
				return
			} else {
				watcher, _ = getWatcher(path)
			}
		}
	}
}
