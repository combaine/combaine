package main

import (
	"github.com/noxiouz/Combaine/combainer"
	"log"
	"time"
)

func Work() {
	cl, err := combainer.NewClient(combainer.COMBAINER_PATH)
	if err != nil {
		log.Println("AAAAA", err)
		return
	}
	log.Println("Create client", cl)
	cl.Dispatch()
}

func main() {
	for {
		log.Println("Try to start client")
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Println("Recovered in f", r)
				}
			}()
			Work()
		}()
		time.Sleep(time.Second * 5)
	}
}
