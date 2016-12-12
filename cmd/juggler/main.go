package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/senders/juggler"
)

var logger *cocaine.Logger

type task struct {
	ID     string
	Data   []tasks.AggregationResult
	Config juggler.JugglerConfig
}

func send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task Task
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

	sConf, err := juggler.GetJugglerSenderConfig()
	if err != nil {
		logger.Errf("%s Failed to read juggler config %s", err, task.ID)
		response.ErrorMsg(-100, err.Error())
		return
	}
	if sConf.Frontend == nil {
		sConf.Frontend = sConf.Hosts
	}
	if task.Config.JHosts == nil {
		if sConf.Hosts == nil {
			msg := fmt.Sprintf("%s juggler hosts not defined", task.ID)
			logger.Err(msg)
			response.ErrorMsg(-100, msg)
			return
		}
		// if jhosts not in PluginConfig override both jhosts and jfrontend
		task.Config.JHosts = sConf.Hosts
		task.Config.JFrontend = sConf.Frontend
	} else {
		if task.Config.JFrontend == nil {
			// jhost is by default used as jfrontend
			task.Config.JFrontend = task.Config.JHosts
		}
	}
	if task.Config.PluginsDir == "" {
		task.Config.PluginsDir = sConf.PluginsDir
	}

	logger.Debugf("%s Task: %v", task, task.ID)

	jCli, err := juggler.NewJugglerSender(&task.Config, task.ID)
	if err != nil {
		logger.Errf("%s Unexpected error %s", err, task.ID)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = jCli.Send(context.Background(), task.Data)
	if err != nil {
		logger.Errf("%s Sending error %s", err, task.ID)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
	logger, _ = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"send": send,
	}
	logger.Debugf("Start Juggler cocaine worker")

	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
