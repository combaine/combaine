package main

import (
	"log"
	"runtime"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/senders/graphite"
)

var DEFAULT_FIELDS = []string{"75_prc", "90_prc", "93_prc", "94_prc", "95_prc", "96_prc", "97_prc", "98_prc", "99_prc"}

var logger *cocaine.Logger

type Task struct {
	Data   tasks.DataType
	Config graphite.GraphiteCfg
}

func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task Task
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	logger.Debugf("Task: %v", task)

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = DEFAULT_FIELDS
	}

	gCli, err := graphite.NewGraphiteClient(&task.Config)
	if err != nil {
		logger.Errf("Unexpected error %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = gCli.Send(task.Data)
	if err != nil {
		logger.Errf("Sending error %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
	runtime.GOMAXPROCS(10)
	var err error
	logger, err = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
