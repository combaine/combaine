package main

import (
	"log"

	"github.com/Combaine/Combaine/aggregating"

	"github.com/Combaine/Combaine/common"
	"github.com/Combaine/Combaine/common/servicecacher"
	"github.com/Combaine/Combaine/common/tasks"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

var cacher servicecacher.Cacher = servicecacher.NewCacher()

func handleTask(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	raw := <-request.Read()
	var task tasks.AggregationTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	err = aggregating.Aggregating(&task, cacher)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("OK")
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
