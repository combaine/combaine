package main

import (
	"log"
	"runtime"

	"github.com/noxiouz/Combaine/aggregating"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/tasks"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

func handleTask(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	raw := <-request.Read()
	var task tasks.AggregationTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	err = aggregating.Aggregating(task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
	} else {
		response.Write("OK")
	}
}

func main() {
	runtime.GOMAXPROCS(10)
	binds := map[string]cocaine.EventHandler{
		"handleTask": handleTask,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
