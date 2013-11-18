package main

import (
	"log"

	"github.com/noxiouz/Combaine/parsing"
)

func main() {
	t := parsing.Task{"imagick01g.photo.yandex.ru", "photo_proxy", "testgroup", 0, 20, "UNIQUEID"}
	log.Println(parsing.Parsing(t))
}
