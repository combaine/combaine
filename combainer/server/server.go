package server

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/serf/serf"
	"github.com/talbright/go-zookeeper/zk"

	"github.com/combaine/combaine/combainer"
	"github.com/combaine/combaine/combainer/lockserver"
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

type CombaineServer struct {
	Configuration   CombaineServerConfig
	CombainerConfig configs.CombainerConfig
	configs.Repository

	// serfEventCh is used to receive events from the serf cluster
	serfEventCh chan serf.Event
	// instance of Serf
	Serf       *serf.Serf
	shutdownCh chan struct{}

	cache.Cache
	*combainer.Context
	log *logrus.Entry
}

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

func NewCombainer(config CombaineServerConfig) (*CombaineServer, error) {
	log := logrus.WithField("source", "server")
	repository, err := configs.NewFilesystemRepository(config.ConfigsPath)
	if err != nil {
		log.Fatalf("unable to initialize filesystemRepository: %s", err)
	}

	combainerConfig := repository.GetCombainerConfig()
	if err = configs.VerifyCombainerConfig(&combainerConfig); err != nil {
		log.Fatalf("malformed combainer config: %s", err)
	}

	cacheCfg := &combainerConfig.MainSection.Cache
	cacheType, err := cacheCfg.Type()
	if err != nil {
		log.Fatalf("unable to get type of cache: %s", err)
	}

	cacher, err := cache.NewCache(cacheType, cacheCfg)
	if err != nil {
		log.Fatalf("unable to initialize cache: %s", err)
	}

	// Get Combaine hosts
	context := &combainer.Context{
		Cache:  cacher,
		Hosts:  nil,
		Logger: logrus.StandardLogger(),
	}

	s, err := combainer.LoadHostFetcher(context, combainerConfig.CloudSection.HostFetcher)
	if err != nil {
		return nil, err
	}

	context.Hosts = func() ([]string, error) {
		h, err := s.Fetch(combainerConfig.MainSection.CloudGroup)
		if err != nil {
			return nil, err
		}
		return h.AllHosts(), nil
	}

	server := &CombaineServer{
		Configuration:   config,
		CombainerConfig: combainerConfig,
		Repository:      repository,
		serfEventCh:     make(chan serf.Event, 256),
		shutdownCh:      make(chan struct{}),
		Cache:           cacher,
		Context:         context,
		log:             log,
	}
	server.Serf, err = server.setupSerf()
	if err != nil {
		if server.Serf != nil {
			server.Serf.Shutdown()
		}
		return nil, fmt.Errorf("Failed to start serf: %v", err)
	}

	return server, nil
}

func (c *CombaineServer) GetContext() *combainer.Context {
	return c.Context
}

func (c *CombaineServer) GetRepository() configs.Repository {
	return c.Repository
}

func (c *CombaineServer) Serve() error {
	c.log.Info("starting REST API")
	router := combainer.GetRouter(c)
	go func() {
		err := http.ListenAndServe(c.Configuration.RestEndpoint, router)
		if err != nil {
			c.log.Fatal("ListenAndServe: ", err)
		}
	}()

	c.log.Info("Join to Serf cluster")
	hosts, nil := c.Context.Hosts()
	go c.Serf.Join(hosts, true)

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

		var next <-chan time.Time
		next = time.After(time.Millisecond * 10)

	DISPATCH_LOOP:
		for {
			select {
			// Spawn one more client
			case <-next:
				next = time.After(c.Configuration.Period)

				configs, err := c.Repository.ListParsingConfigs()
				if err != nil {
					c.log.WithFields(logrus.Fields{
						"error": err,
					}).Error("unable to list parsing configs")
					continue DISPATCH_LOOP
				}

				go func(configs []string) {

					for _, cfg := range configs {
						lockerr := DLS.Lock(cfg)
						if lockerr != nil {
							continue
						}

						lockname := cfg

						// Inline function to use defers
						func(lockname string) {
							defer DLS.Unlock(lockname)
							defer trap()

							if !c.Repository.ParsingConfigIsExists(cfg) {
								c.log.WithField("error", "config doesn't exist").Error(cfg)
								return
							}

							c.log.Infof("creating new client %s", lockname)
							cl, err := combainer.NewClient(c.Context, c.Repository)
							if err != nil {
								c.log.WithFields(logrus.Fields{
									"error":    err,
									"lockname": lockname,
								}).Error("can't create client")
								return
							}

							watcher, err := DLS.Watch(lockname)
							if err != nil {
								c.log.WithFields(logrus.Fields{
									"error":    err,
									"lockname": lockname,
								}).Error("can't create watch")
								return
							}

							for {
								if err = cl.Dispatch(lockname, genUniqueID, shouldWait); err != nil {
									c.log.WithFields(logrus.Fields{
										"error":    err,
										"lockname": lockname,
									}).Error("Dispatch error")
									return
								}
								select {
								case event := <-watcher:
									if event.Err != nil || event.Type == zk.EventNodeDeleted {
										c.log.Errorf("lock has been lost: %s", event)
										return
									}
									watcher, err = DLS.Watch(lockname)
									if err != nil {
										c.log.WithFields(logrus.Fields{
											"error":    err,
											"lockname": lockname,
										}).Error("can't continue watching")
										return
									}
								default:
								}
							}
						}(lockname)
					}
				}(configs)
			case event := <-DLS.Session:
				if event.State == zk.StateConnecting {
					c.log.Warn("reconnecting event from Zookeeper session")
					continue
				}

				if event.Err != nil {
					c.log.Errorf("event with error from Zookeeper: %v", event.Err)
					DLS.Close()
					break DISPATCH_LOOP
				}
			}

		}
	}

}
