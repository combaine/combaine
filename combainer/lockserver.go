package combainer

import (
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"launchpad.net/gozk/zookeeper"

	"github.com/noxiouz/Combaine/common/configs"
)

type LockServer struct {
	Zk      *zookeeper.Conn
	Session <-chan zookeeper.Event
	stop    chan struct{}
	configs.LockServerSection
}

func NewLockServer(config configs.LockServerSection) (*LockServer, error) {
	endpoints := strings.Join(config.Hosts, ",")
	log.Infof("Zookeeper: connecting to %s", endpoints)
	zk, session, err := zookeeper.Dial(endpoints, 5e9)
	if err != nil {
		log.Errorf("Zookeeper: unable to connect to %s %s", endpoints, err)
		return nil, err
	}

ZK_CONNECTING_WAIT_LOOP:
	for {
		select {
		case event := <-session:
			if !event.Ok() {
				err = fmt.Errorf("%s", event.String())
				log.Errorf("Zookeeper connection error: %s", err)
				return nil, err
			}

			switch event.State {
			case zookeeper.STATE_CONNECTED:
				log.Infof("Connected to Zookeeper successfully")
				break ZK_CONNECTING_WAIT_LOOP
			case zookeeper.STATE_CONNECTING:
				log.Infof("Connecting to Zookeeper...")
			default:
				log.Warningf("Unexpectable Zookeeper session event: %s", event)
			}
		case <-time.After(5 * time.Second):
			zk.Close()
			return nil, fmt.Errorf("Zookeeper: connection timeout")
		}
	}

	ls := &LockServer{
		Zk:                zk,
		Session:           session,
		stop:              make(chan struct{}),
		LockServerSection: config,
	}

	return ls, nil
}

func (ls *LockServer) Lock(node string) error {
	path := fmt.Sprintf("/%s/%s", ls.LockServerSection.Id, node)
	log.Infof("Locking %s", path)
	content, err := os.Hostname()
	if err != nil {
		return err
	}
	_, err = ls.Zk.Create(path, content, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return err
}

func (ls *LockServer) Unlock(node string) error {
	path := fmt.Sprintf("/%s/%s", ls.LockServerSection.Id, node)
	log.Infof("Unlocking %s", path)
	return ls.Zk.Delete(path, -1)
}

func (ls *LockServer) Watch(node string) (<-chan zookeeper.Event, error) {
	path := fmt.Sprintf("/%s/%s", ls.LockServerSection.Id, node)
	_, _, w, err := ls.Zk.GetW(path)
	return w, err
}

func (ls *LockServer) Close() {
	close(ls.stop)
	ls.Zk.Close()
}
