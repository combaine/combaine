package main

import (
	"context"
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/senders/juggler"
)

const defaultPlugin = "simple"

type senderTask struct {
	ID     string `codec:"Id"`
	Data   []common.AggregationResult
	Config juggler.Config
}

func send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task senderTask
	err := common.Unpack(raw, &task)
	if err != nil {
		logger.Errf("%s Failed to unpack juggler task %s", task.ID, err)
		return
	}
	// common.Unpack unpack some strings as []byte, need convert it
	juggler.StringifyAggregatorLimits(task.Config.AggregatorKWArgs.Limits)
	task.Config.Tags = juggler.EnsureDefaultTag(task.Config.Tags)

	sConf, err := juggler.GetSenderConfig()
	if err != nil {
		logger.Errf("%s Failed to read juggler config %s", task.ID, err)
		return
	}
	if sConf.Frontend == nil {
		sConf.Frontend = sConf.Hosts
	}
	if task.Config.JHosts == nil {
		if sConf.Hosts == nil {
			logger.Errf("%s juggler hosts not defined", task.ID)
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
	if task.Config.Plugin == "" {
		task.Config.Plugin = defaultPlugin
	}
	juggler.GlobalCache.TuneCache(sConf.CacheTTL, sConf.CacheCleanInterval)
	logger.Debugf("%s Task: %v", task.ID, task)

	jCli, err := juggler.NewSender(&task.Config, task.ID)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.ID, err)
		return
	}

	err = jCli.Send(context.Background(), task.Data)
	if err != nil {
		logger.Errf("%s Sending error %s", task.ID, err)
		return
	}
	response.Write("DONE")
}

func main() {
	juggler.InitializeLogger()

	binds := map[string]cocaine.EventHandler{
		"send": send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
