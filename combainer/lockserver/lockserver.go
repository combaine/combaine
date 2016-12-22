package lockserver

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/talbright/go-zookeeper/zk"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
)

// LockServer object represent zk connection
type LockServer struct {
	log     *logrus.Entry
	Conn    *zk.Conn
	Session <-chan zk.Event
	configs.LockServerSection
}

// NewLockServer attempt connecting to zk server
// and return LockServer with connection or err
func NewLockServer(config configs.LockServerSection) (*LockServer, error) {
	log := logrus.WithField("source", "zookeeper")
	log.Infof("connecting to %s", config.Hosts)
	connTimeout := time.Duration(config.Timeout) * time.Second
	conn, events, err := zk.Connect(config.Hosts, 5*time.Second,
		zk.WithConnectTimeout(connTimeout),
		zk.WithLogger(log),
		zk.WithDialer(func(network, address string, timeout time.Duration) (net.Conn, error) {
			dialer := net.Dialer{
				Timeout:   timeout,
				DualStack: true,
			}
			return dialer.Dial(network, address)
		}))
	if err != nil {
		log.Errorf("Zookeeper: unable to connect to %s %s", config.Hosts, err)
		return nil, err
	}

	attempts := 5
	for event := range events {
		switch event.State {
		case zk.StateHasSession:
			ls := &LockServer{
				log:               log,
				Conn:              conn,
				Session:           events,
				LockServerSection: config,
			}
			return ls, nil
		case zk.StateConnecting:
			if attempts--; attempts <= 0 {
				conn.Close()
				return nil, fmt.Errorf("Connection attempts limit is reached")
			}
		default:
			attempts = 5
			log.Infof("get connecting event %s", event)
		}
	}

	conn.Close()
	return nil, err
}

// Lock try create ephemeral node and lock it
func (ls *LockServer) Lock(node string) error {
	path := filepath.Join("/", ls.LockServerSection.Id, node)
	ls.log.Infof("Locking %s", path)
	content, err := os.Hostname()
	if err != nil {
		return err
	}

	// check for node exists before create save many io on zk leader
	exists, state, err := ls.Conn.Exists(path)
	if err == nil && exists {
		if state.EphemeralOwner != ls.Conn.SessionID() {
			return common.ErrLockByAnother
		}
		return common.ErrLockOwned
	}
	_, err = ls.Conn.Create(path, []byte(content), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))

	return err
}

// Unlock try remove owned ephemeral node
func (ls *LockServer) Unlock(node string) error {
	path := filepath.Join("/", ls.LockServerSection.Id, node)
	ls.log.Infof("Unlocking %s", path)
	if exists, state, err := ls.Conn.Exists(path); err == nil && exists {
		if state.EphemeralOwner == ls.Conn.SessionID() {
			return ls.Conn.Delete(path, -1)
		}
		ls.log.Debugf("Node %s locked by another server", path)
	}
	return nil
}

// Watch watch events from ephemeral node
func (ls *LockServer) Watch(node string) (<-chan zk.Event, error) {
	path := filepath.Join("/", ls.LockServerSection.Id, node)
	_, _, w, err := ls.Conn.GetW(path)
	return w, err
}

// Close interrupt zk connection
func (ls *LockServer) Close() {
	ls.Conn.Close()
}
