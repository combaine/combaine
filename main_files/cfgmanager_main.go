package main

import (
	"fmt"
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/configmanager"
)

func parsing(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	raw := <-request.Read()
	name := string(raw)
	if data, err := configmanager.GetParsingCfg(name); err != nil {
		response.ErrorMsg(-2, fmt.Sprintf("Missing file with name %s", name))
	} else {
		response.Write(data)
	}
}

func aggregate(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	raw := <-request.Read()
	name := string(raw)
	if data, err := configmanager.GetAggregateCfg(name); err != nil {
		response.ErrorMsg(-2, fmt.Sprintf("Missing file with name %s", name))
	} else {
		response.Write(data)
	}
}

func common(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	<-request.Read()
	if data, err := configmanager.GetCommonCfg(); err != nil {
		response.ErrorMsg(-2, "Missing combaine.(yaml|json)")
	} else {
		response.Write(data)
	}

}

func main() {
	binds := map[string]cocaine.EventHandler{
		"parsing":   parsing,
		"aggregate": aggregate,
		"common":    common,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
