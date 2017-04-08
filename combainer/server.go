package combainer

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/configs"
)

// CombaineServer main combaine object
type CombaineServer struct {
	Configuration   CombaineServerConfig
	CombainerConfig configs.CombainerConfig
	repository      configs.Repository

	cluster    *Cluster
	shutdownCh chan struct{}

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

// New create new combainer server
func New(config CombaineServerConfig) (*CombaineServer, error) {
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

	server := &CombaineServer{
		Configuration:   config,
		CombainerConfig: combainerConfig,
		repository:      repository,
		Cache:           cacher,
		log:             log,
	}

	server.cluster, err = NewCluster(cacher, repository, combainerConfig.MainSection.ClusterConfig)
	if err != nil {
		return nil, err
	}
	return server, nil
}

// GetRepository return repository of configs
func (c *CombaineServer) GetRepository() configs.Repository {
	return c.repository
}

// GetHosts return alive cluster members
func (c *CombaineServer) GetHosts() []string {
	return c.cluster.Hosts()
}

// GetCache return combainer cache
func (c *CombaineServer) GetCache() cache.Cache {
	return c.Cache
}

// Serve run main event loop
func (c *CombaineServer) Serve() error {
	defer c.cluster.Shutdown()

	c.log.Info("Starting REST API")
	router := GetRouter(c)
	go func() {
		err := http.ListenAndServe(c.Configuration.RestEndpoint, router)
		if err != nil {
			c.log.Fatal("ListenAndServe: ", err)
		}
	}()

	f, err := LoadHostFetcher(c.Cache, c.CombainerConfig.CloudSection.HostFetcher)
	if err != nil {
		return err
	}
	hostsByDc, err := f.Fetch(c.CombainerConfig.MainSection.CloudGroup)
	if err != nil {
		return fmt.Errorf("Failed to fetch cloud group: %s", err)
	}
	hosts := hostsByDc.RemoteHosts()

	if err := c.cluster.Bootstrap(hosts, c.Configuration.Period); err != nil {
		c.log.Errorf("Failed to connect cluster: %s", err)
		return err
	}

	c.log.Info("start task distribution")
	go c.cluster.Run()

	sigWatcher := make(chan os.Signal, 1)
	signal.Notify(sigWatcher, os.Interrupt, os.Kill)
	sig := <-sigWatcher
	c.log.Info("Got signal:", sig)
	return nil
}
