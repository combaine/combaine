package main

import (
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/Combaine/Combaine/common"
	"github.com/Combaine/Combaine/common/tasks"
	"github.com/Combaine/Combaine/parsing"

	_ "github.com/Combaine/Combaine/fetchers/httpfetcher"
	_ "github.com/Combaine/Combaine/fetchers/rawsocket"
	_ "github.com/Combaine/Combaine/fetchers/timetail"
)

var logger *cocaine.Logger

func handleTask(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	raw := <-request.Read()
	var task tasks.ParsingTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	result, err := parsing.Parsing(task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
	} else {
		res, _ := common.Pack(result)
		response.Write(res)
	}
}

func main() {
	var err error
	logger, err = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"handleTask": handleTask,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
