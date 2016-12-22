package server

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/serf/serf"
	"github.com/talbright/go-zookeeper/zk"

	"github.com/combaine/combaine/combainer"
	"github.com/combaine/combaine/combainer/lockserver"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/configs"
)

var (
	shouldWait  = true
	genUniqueID = ""
)

func trap() {
	if r := recover(); r != nil {
		logrus.Printf("Recovered: %s", r)
	}
}

// CombaineServer main combaine object
type CombaineServer struct {
	Configuration   CombaineServerConfig
	CombainerConfig configs.CombainerConfig
	context         *combainer.Context

	// serfEventCh is used to receive events from the serf cluster
	serfEventCh chan serf.Event
	// instance of Serf
	Serf       *serf.Serf
	shutdownCh chan struct{}

	configs.Repository
	cache.Cache
	log *logrus.Entry
}

// CombaineServerConfig contains config from main combaine conf
type CombaineServerConfig struct {
	// Configuration
	// path to directory with combaine.yaml
	ConfigsPath string
	// period of the locks rechecking
	Period time.Duration
	// Addrto listen for incoming http REST API requests
	RestEndpoint string
	//
	Active bool
}

// NewCombainer create new combaine server
func NewCombainer(config CombaineServerConfig) (*CombaineServer, error) {
	log := logrus.WithField("source", "server")
	repository, err := configs.NewFilesystemRepository(config.ConfigsPath)
	if err != nil {
		log.Fatalf("unable to initialize filesystemRepository: %s", err)
	}
	log.Info("filesystemRepository initialized")

	combainerConfig := repository.GetCombainerConfig()
	if err = configs.VerifyCombainerConfig(&combainerConfig); err != nil {
		log.Fatalf("malformed combainer config: %s", err)
	}
	log.Info("Combainer configs is valid: OK")

	cacheCfg := &combainerConfig.MainSection.Cache
	cacheType, err := cacheCfg.Type()
	if err != nil {
		log.Fatalf("unable to get type of cache: %s", err)
	}

	cacher, err := cache.NewCache(cacheType, cacheCfg)
	if err != nil {
		log.Fatalf("unable to initialize cache: %s", err)
	}
	log.Infof("Initialized combainer cache type: %s", cacheType)

	context := &combainer.Context{
		Cache:  cacher,
		Serf:   nil,
		Logger: logrus.StandardLogger(),
	}

	server := &CombaineServer{
		Configuration:   config,
		CombainerConfig: combainerConfig,
		Repository:      repository,
		serfEventCh:     make(chan serf.Event, 256),
		shutdownCh:      make(chan struct{}),
		Cache:           cacher,
		context:         context,
		log:             log,
	}

	server.Serf, err = server.setupSerf()
	if err != nil {
		if server.Serf != nil {
			server.Serf.Shutdown()
		}
		log.Fatalf("Failed to start serf: %s", err)
	}
	server.context.Serf = server.Serf

	return server, nil
}

// GetContext return servers context
func (c *CombaineServer) GetContext() *combainer.Context {
	return c.context
}

// GetRepository return repository of configs
func (c *CombaineServer) GetRepository() configs.Repository {
	return c.Repository
}

// Serve run main event loop
func (c *CombaineServer) Serve() error {
	c.log.Info("starting REST API")
	router := combainer.GetRouter(c)
	go func() {
		err := http.ListenAndServe(c.Configuration.RestEndpoint, router)
		if err != nil {
			c.log.Fatal("ListenAndServe: ", err)
		}
	}()

	// run Serf instance and monitor for this events
	if err := c.connectSerf(); err != nil {
		c.log.Errorf("Failed to connectSerf: %s", err)
	}
	go c.serfEventHandler()

	if c.Configuration.Active {
		c.log.Info("start task distribution")
		go c.distributeTasks()
	}

	sigWatcher := make(chan os.Signal, 1)
	signal.Notify(sigWatcher, os.Interrupt, os.Kill)
	sig := <-sigWatcher
	c.log.Info("Got signal:", sig)
	close(c.shutdownCh)
	return nil
}

func (c *CombaineServer) distributeTasks() {
LOCKSERVER_LOOP:
	for {
		DLS, err := lockserver.NewLockServer(c.CombainerConfig.LockServerSection)
		if err != nil {
			c.log.WithFields(logrus.Fields{
				"error": err,
			}).Error("unable to create Zookeeper lockserver")
			time.Sleep(c.Configuration.Period)
			continue LOCKSERVER_LOOP
		}

		var iteration uint64
		var next <-chan time.Time
		next = time.After(time.Millisecond * 10)

		breakDispatchLoop := make(chan struct{})
		var breakGuard sync.Once

	DISPATCH_LOOP:
		for {
			iteration++
			ilog := c.log.WithField("iteration", strconv.FormatUint(iteration, 10))
			select {
			// Spawn one more client
			case <-next:
				next = time.After(c.Configuration.Period)

				configs, err := c.Repository.ListParsingConfigs()
				if err != nil {
					ilog.WithField("error", err).Error("unable to list parsing configs")
					continue DISPATCH_LOOP
				}

				go func(configs []string, stop chan struct{}, guard *sync.Once) {
					var err error

				LOCK_LOOP:
					for _, lockname := range configs {
						llog := ilog.WithField("lockname", lockname)
						lockerr := DLS.Lock(lockname /* lockname is config file name*/)
						switch lockerr {
						case nil:
							llog.Debug("Lock acquired")
						case common.ErrLockByAnother:
							llog.Debug(lockerr)
							continue
						case common.ErrLockOwned:
							if combainer.GlobalObserver.HasClient(lockname) {
								llog.Debug(lockerr)
								continue
							}
							llog.Debug("Lock acquired")
						default:
							err = fmt.Errorf("Lock failed %s", lockerr)
							break LOCK_LOOP
						}

						// Inline function to use defers
						err := func(name string) error {
							defer trap()
							defer DLS.Unlock(name)

							if !c.Repository.ParsingConfigIsExists(name) {
								llog.Error("config doesn't exist")
								return nil
							}

							llog.Info("creating new client")
							cl, err := combainer.NewClient(c.context, c.Repository)
							if err != nil {
								llog.Errorf("can't create client %s", err)
								return nil
							}

							watcher, err := DLS.Watch(name)
							if err != nil {
								llog.Errorf("can't create watch %s", err)
								return err
							}
							atomic.AddInt32(&combainer.GlobalObserver.WatchersCount, 1)
							defer atomic.AddInt32(&combainer.GlobalObserver.WatchersCount, -1)

							for {
								if err = cl.Dispatch(name, genUniqueID, shouldWait); err != nil {
									llog.Errorf("Dispatch error %s", err)
									return nil
								}
								switch state := DLS.Conn.State(); state {
								case zk.StateHasSession, zk.StateConnected:
									// ok continue
								case zk.StateConnecting:
									llog.Warnf("zk state is %v", state)
									return nil // let reconnection state checked in Watch setup
								default:
									return fmt.Errorf("zk now disconnected, %v", state)
								}

								select {
								case event := <-watcher:
									// stop Dispatching if lock has been force deleted
									if event.Type == zk.EventNodeDeleted {
										llog.Errorf("lock has been deleted: %s", event)
										return nil
									}
									watcher, err = DLS.Watch(name)
									if err != nil {
										// Dispatching will be stop here
										llog.Errorf("lock has been lost %s, can't continue watching %s", event.Err, err)
										return err
									}
								default:
								}
							}
						}(lockname)
						if err != nil {
							break LOCK_LOOP
						}
					}
					if err != nil {
						ilog.Errorf("Stop client: %s", err)
						guard.Do(func() {
							ilog.Info("break dispatch loop")
							DLS.Close() // after Close() all Watch will be stuck
							close(stop)
						})
						return
					}
				}(configs, breakDispatchLoop, &breakGuard)
				select {
				case <-breakDispatchLoop:
					break DISPATCH_LOOP
				default:
				}
			case event := <-DLS.Session:
				if event.Err != nil {
					ilog.Errorf("event with error from Zookeeper: %v", event.Err)
					DLS.Close()
					break DISPATCH_LOOP
				}
			}

		}
	}

}
