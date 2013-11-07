package main

import (
	"log"

	"github.com/noxiouz/Combaine/parsing"
)

func main() {
	t := parsing.Task{"testhost", "photo_proxy.json", "testgroup", 100, 400, "UNIQUEID"}
	log.Println(parsing.Parsing(t))
}
