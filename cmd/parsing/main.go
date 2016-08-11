package main

import (
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/parsing"

	_ "github.com/noxiouz/Combaine/fetchers/httpfetcher"
	_ "github.com/noxiouz/Combaine/fetchers/rawsocket"
	_ "github.com/noxiouz/Combaine/fetchers/timetail"
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
	err = parsing.Parsing(task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
	} else {
		response.Write("OK")
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
