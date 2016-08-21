package lockserver

import (
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/talbright/go-zookeeper/zk"

	"github.com/combaine/combaine/common/configs"
)

type LockServer struct {
	log     *logrus.Entry
	Conn    *zk.Conn
	Session <-chan zk.Event
	configs.LockServerSection
}

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
		default:
			log.Infof("get connecting event %s", event)
		}
	}

	conn.Close()
	return nil, err
}

func (ls *LockServer) Lock(node string) error {
	path := filepath.Join("/", ls.LockServerSection.Id, node)
	ls.log.Infof("Locking %s", path)
	content, err := os.Hostname()
	if err != nil {
		return err
	}
	_, err = ls.Conn.Create(path, []byte(content), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

func (ls *LockServer) Unlock(node string) error {
	path := filepath.Join("/", ls.LockServerSection.Id, node)
	ls.log.Infof("Unlocking %s", path)
	return ls.Conn.Delete(path, -1)
}

func (ls *LockServer) Watch(node string) (<-chan zk.Event, error) {
	path := filepath.Join("/", ls.LockServerSection.Id, node)
	_, _, w, err := ls.Conn.GetW(path)
	return w, err
}

func (ls *LockServer) Close() {
	ls.Conn.Close()
}
