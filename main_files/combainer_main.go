package main

import (
	"flag"
	"log"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/noxiouz/Combaine/combainer"
	"github.com/noxiouz/Combaine/common/cache"
	"github.com/noxiouz/Combaine/common/configs"
)

const (
	CONFIGS_PATH   = "/etc/combaine"
	DEFAULT_PERIOD = 5
)

var (
	endpoint    string
	profiler    string
	logoutput   string
	loglevel    string
	ConfigsPath string
	period      uint
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&profiler, "profiler", "", "profiler host:port <0.0.0.0:10000>")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&loglevel, "loglevel", "INFO", "loglevel (DEBUG|INFO|WARN|ERROR)")
	flag.StringVar(&ConfigsPath, "configspath", CONFIGS_PATH, "path to root of configs")
	flag.UintVar(&period, "period", 5, "period of retrying new lock (sec)")
}

func main() {
	flag.Parse()

	repository, err := configs.NewFilesystemRepository(ConfigsPath)
	if err != nil {
		log.Fatalf("unable to initialize filesystemRepository: %s", err)
	}

	combainerConfig := repository.GetCombainerConfig()

	cacheCfg := &combainerConfig.MainSection.Cache
	log.Println(*cacheCfg)
	cacheType, err := cacheCfg.Type()
	if err != nil {
		log.Fatalf("unable to get type of cache: %s", err)
	}
	cacher, err := cache.NewCache(cacheType, cacheCfg)
	if err != nil {
		log.Fatalf("unable to initialize cache: %s", err)
	}

	combainer.InitializeLogger(loglevel, logoutput)
	if profiler != "" {
		log.Println("Profiler enabled")
		go func() {
			if err := http.ListenAndServe(profiler, nil); err != nil {
				log.Fatal(err)
			}
			log.Println("Launch profiler successfully on ", profiler)
		}()
	}

	context := combainer.Context{
		Cache: cacher,
	}
	go combainer.StartObserver(endpoint)
	for {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered: %s", r)
				}
			}()

			log.Println("Creating new client")
			cl, err := combainer.NewClient(&context, combainerConfig, repository)
			if err != nil {
				log.Panicf("Can't create client: %s", err)
			}
			cl.Dispatch()

		}()
		time.Sleep(time.Second * time.Duration(period))
	}
}
