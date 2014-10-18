package main

import (
	"flag"
	"io/ioutil"
	"log"
	"time"

	"net/http"
	_ "net/http/pprof"

	"launchpad.net/goyaml"

	"github.com/noxiouz/Combaine/combainer"
	"github.com/noxiouz/Combaine/common/configs"
)

const (
	COMBAINER_PATH = "/etc/combaine/combaine.yaml"
	DEFAULT_PERIOD = 5
)

var (
	endpoint            string
	profiler            string
	logoutput           string
	loglevel            string
	CombainerConfigPath string
	period              uint
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&profiler, "profiler", "", "profiler host:port <0.0.0.0:10000>")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&loglevel, "loglevel", "INFO", "loglevel (DEBUG|INFO|WARN|ERROR)")
	flag.StringVar(&CombainerConfigPath, "CombainerConfigPath", COMBAINER_PATH, "path to combainer.yaml")
	flag.UintVar(&period, "period", 5, "period of retrying new lock (sec)")
}

func Work() {
	log.Println("Creating new client")
	data, err := ioutil.ReadFile(CombainerConfigPath)
	if err != nil {
		log.Panicf("Unable to read combaine.yaml: %s", err)
	}

	var config configs.CombainerConfig
	if err = goyaml.Unmarshal(data, &config); err != nil {
		log.Panicf("Unable to decode combaine.yaml: %s", err)
	}

	cl, err := combainer.NewClient(config)
	if err != nil {
		log.Panicf("Can't create client: %s", err)
	}
	cl.Dispatch()
}

func main() {
	flag.Parse()
	combainer.InitializeCacher()
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

	go combainer.StartObserver(endpoint)
	for {
		//log.Println("Try to start client")
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Println("Recovered in f", r)
				}
			}()
			Work()
		}()
		time.Sleep(time.Second * time.Duration(period))
	}
}
