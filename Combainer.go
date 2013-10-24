package main

import (
	"combainer"
	"log"
)

func main() {
	cl, err := combainer.NewClient(combainer.COMBAINER_PATH)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Create client", cl)
	cl.Dispatch()
}
