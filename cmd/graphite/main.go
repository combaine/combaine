package main

import (
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/senders/graphite"
)

var (
	defaultFields = []string{
		"75_prc",
		"90_prc",
		"93_prc",
		"94_prc",
		"95_prc",
		"96_prc",
		"97_prc",
		"98_prc",
		"99_prc",
	}
	defaultConnectionEndpoint = ":42000"
	logger                    *cocaine.Logger
)

type graphiteTask struct {
	ID       string `codec:"Id"`
	Data     []tasks.AggregationResult
	Config   graphite.Config
	CurrTime uint64
	PrevTime uint64
}

// Send parse cocaine request and send points to graphite
func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task graphiteTask
	err := common.Unpack(raw, &task)
	if err != nil {
		logger.Errf("%s Failed to unpack graphite task %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	logger.Debugf("%s Task: %v", task.ID, task)

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = defaultFields
	}
	if task.Config.Endpoint == "" {
		task.Config.Endpoint = defaultConnectionEndpoint
	}

	gCli, err := graphite.NewSender(&task.Config, task.ID)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = gCli.Send(task.Data, task.PrevTime)
	if err != nil {
		logger.Errf("%s Sending error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
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
