package main

import (
	"flag"
	"log"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/noxiouz/Combaine/combainer"
)

var (
	endpoint  string
	profiler  string
	logoutput string
	loglevel  string
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&profiler, "profiler", "", "profiler host:port <0.0.0.0:10000>")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&loglevel, "loglevel", "INFO", "loglevel (DEBUG|INFO|WARN|ERROR)")
}

func Work() {
	cl, err := combainer.NewClient(combainer.COMBAINER_PATH)
	if err != nil {
		//log.Printf("Can't create client: %s", err)
		return
	}
	//log.Println("Create client", cl)
	cl.Dispatch()
}

func main() {
	flag.Parse()
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
		time.Sleep(time.Second * 150)
	}
}
