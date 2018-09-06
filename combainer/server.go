package combainer

import (
	"errors"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/repository"
)

var combainerCache *cache.TTLCache

// CombaineServer main combaine object
type CombaineServer struct {
	Configuration   CombaineServerConfig
	CombainerConfig repository.CombainerConfig

	cluster    *Cluster
	shutdownCh chan struct{}

	log *logrus.Entry
}

// CombaineServerConfig contains config from main combaine conf
type CombaineServerConfig struct {
	// period of the locks rechecking
	Period time.Duration
	// Addrto listen for incoming http REST API requests
	RestEndpoint string
	//
	Active bool
}

// New create new combainer server
func New(config CombaineServerConfig) (*CombaineServer, error) {
	var err error
	log := logrus.WithField("source", "server")
	combainerConfig := repository.GetCombainerConfig()
	if err = repository.VerifyCombainerConfig(&combainerConfig); err != nil {
		log.Fatalf("malformed combainer config: %s", err)
	}
	log.Info("Combainer configs is valid: OK")

	ttl := time.Duration(combainerConfig.MainSection.Cache.TTL) * time.Minute
	interval := time.Duration(combainerConfig.MainSection.Cache.Interval) * time.Minute
	combainerCache = cache.NewCache(ttl, interval, interval*10)
	log.Infof("Initialized combainer cache: %T", combainerCache)

	server := &CombaineServer{
		Configuration:   config,
		CombainerConfig: combainerConfig,
		log:             log,
	}

	server.cluster, err = NewCluster(combainerConfig.MainSection.ClusterConfig)
	if err != nil {
		return nil, err
	}
	return server, nil
}

// GetHosts return alive cluster members
func (c *CombaineServer) GetHosts() []string {
	return c.cluster.Hosts()
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
	fetcherConfig := c.CombainerConfig.MainSection.HostFetcher
	if len(fetcherConfig) == 0 {
		fetcherConfig = c.CombainerConfig.CloudSection.HostFetcher
	}

	f, err := LoadHostFetcher(fetcherConfig)
	if err != nil {
		return err
	}
	hosts := make([]string, 0)
	for _, group := range c.CombainerConfig.MainSection.CloudGroups {
		hostsByDc, err := f.Fetch(group)
		if err != nil {
			c.log.Errorf("Failed to fetch cloud group: %s", err)
			continue
		}
		hosts = append(hosts, hostsByDc.RemoteHosts()...)

	}
	if len(hosts) == 0 {
		return errors.New("There are no combine operators here")
	}

	if err := c.cluster.createRaftTransport(); err != nil {
		return err
	}
	c.cluster.joinSerf(hosts)

	c.log.Info("start task distribution")
	go c.cluster.Run()

	sigWatcher := make(chan os.Signal, 1)
	signal.Notify(sigWatcher, os.Interrupt, os.Kill)
	sig := <-sigWatcher
	c.log.Info("Got signal:", sig)
	return nil
}
