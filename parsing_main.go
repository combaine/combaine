package main

import (
	"log"
	"runtime"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/parsing"
)

var logger *cocaine.Logger

func handleTask(request *cocaine.Request, response *cocaine.Response) {
	raw := <-request.Read()
	var task common.ParsingTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	err = parsing.Parsing(task)
	if err != nil {
		response.Write(err.Error())
	} else {
		response.Write("OK")
	}
	response.Close()
}

func main() {
	runtime.GOMAXPROCS(10)
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
