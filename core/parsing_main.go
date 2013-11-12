package main

import (
	"log"

	"github.com/noxiouz/Combaine/parsing"
)

func main() {
	t := parsing.Task{"imagick01g.photo.yandex.ru", "photo_proxy.json", "testgroup", 100, 400, "UNIQUEID"}
	log.Println(parsing.Parsing(t))
}
