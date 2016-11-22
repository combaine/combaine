package main

import (
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/senders/juggler"
)

var logger *cocaine.Logger

type Task struct {
	Id       string
	Data     tasks.DataType
	Config   juggler.JugglerConfig
	CurrTime uint64
	PrevTime uint64
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
		logger.Errf("Failed to read juggler config %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	if jugglerServers.Frontend == nil {
		jugglerServers.Frontend = jugglerServers.Hosts
	}

	if task.Config.JHosts == nil {
		if jugglerServers.Hosts == nil {
			msg := "juggler hosts not defined, there is no place to send events"
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

	logger.Debugf("Task: %v", task)

	jCli, err := juggler.NewJugglerClient(&task.Config, task.Id)
	if err != nil {
		logger.Errf("Unexpected error %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = jCli.Send(task.Data)
	if err != nil {
		logger.Errf("Sending error %s", err)
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
