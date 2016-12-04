package main

import (
	"fmt"
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/senders/juggler"
)

var logger *cocaine.Logger

type Task struct {
	Id     string
	Data   tasks.DataType
	Config juggler.JugglerConfig
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

	jugglerServers, err := juggler.GetJugglerConfig()
	if err != nil {
		logger.Errf("%s Failed to read juggler config %s", err, task.Id)
		response.ErrorMsg(-100, err.Error())
		return
	}
	if jugglerServers.Frontend == nil {
		jugglerServers.Frontend = jugglerServers.Hosts
	}

	if task.Config.JHosts == nil {
		if jugglerServers.Hosts == nil {
			msg := fmt.Sprintf("%s juggler hosts not defined", task.Id)
			logger.Err(msg)
			response.ErrorMsg(-100, msg)
			return
		}
		// if jhosts not in PluginConfig override both jhosts and jfrontend
		task.Config.JHosts = jugglerServers.Hosts
		task.Config.JFrontend = jugglerServers.Frontend
	} else {
		if task.Config.JFrontend == nil {
			// jhost is by default used as jfrontend
			task.Config.JFrontend = task.Config.JHosts
		}
	}

	logger.Debugf("%s Task: %v", task, task.Id)

	jCli, err := juggler.NewJugglerSender(task.Config, task.Id)
	if err != nil {
		logger.Errf("%s Unexpected error %s", err, task.Id)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = jCli.Send(task.Data)
	if err != nil {
		logger.Errf("%s Sending error %s", err, task.Id)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
	logger, _ = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	logger.Debugf("Start Juggler cocaine worker")

	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
