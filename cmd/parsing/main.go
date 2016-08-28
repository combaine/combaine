package main

import (
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/parsing"

	_ "github.com/combaine/combaine/fetchers/httpfetcher"
	_ "github.com/combaine/combaine/fetchers/rawsocket"
	_ "github.com/combaine/combaine/fetchers/timetail"
)

var cacher = servicecacher.NewCacher(servicecacher.NewService)

func handleTask(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	raw := <-request.Read()
	var task tasks.ParsingTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	result, err := parsing.Parsing(&task, cacher)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	res, _ := common.Pack(result)
	response.Write(res)
}

func main() {
	binds := map[string]cocaine.EventHandler{
		"handleTask": handleTask,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
