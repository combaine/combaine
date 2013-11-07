package main

import (
	"log"

	"github.com/noxiouz/Combaine/parsing"
)

func main() {
	t := parsing.Task{"cloud01g", "photo_proxy.json", "testgroup", 1, 2, "UNIQUEID"}
	log.Println(parsing.Parsing(t))
}
