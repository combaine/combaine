package main

import (
	"github.com/noxiouz/Combaine/combainer"
	"log"
	"time"
)

func Work() {
	cl, err := combainer.NewClient(combainer.COMBAINER_PATH)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Create client", cl)
	cl.Dispatch()
}

func main() {
	for {
		log.Println("Try to start client")
		go Work()
		time.Sleep(time.Second * 5)
	}
}
