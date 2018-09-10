package main

import (
	"context"
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/senders/juggler"
)

type senderTask struct {
	ID     string `codec:"Id"`
	Data   []common.AggregationResult
	Config juggler.Config
}

var senderConfig *juggler.SenderConfig

func send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task senderTask
	err := common.Unpack(raw, &task)
	if err != nil {
		logger.Errf("%s Failed to unpack juggler task %s", task.ID, err)
		return
	}
	task.Config.Tags = juggler.EnsureDefaultTag(task.Config.Tags)

	err = juggler.UpdateTaskConfig(&task.Config, senderConfig)
	if err != nil {
		logger.Errf("%s Failed to update task config %s", task.ID, err)
		return
	}
	logger.Debugf("%s Task: %v", task.ID, task.Data)
	juggler.AddJugglerToken(&task.Config, senderConfig.Token)

	jCli, err := juggler.NewSender(&task.Config, task.ID)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.ID, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), juggler.DefaultTimeout)
	defer cancel()
	err = jCli.Send(ctx, task.Data)
	if err != nil {
		logger.Errf("%s Sending error %s", task.ID, err)
		return
	}
	response.Write("DONE")
}

func main() {
	var err error
	senderConfig, err = juggler.GetSenderConfig()
	if err != nil {
		log.Fatalf("Failed to load sender config %s", err)
	}

	juggler.InitializeLogger(logger.MustCreateLogger)

	err = repository.Init(juggler.GetConfigDir())
	if err != nil {
		log.Fatalf("unable to initialize filesystemRepository: %s", err)
	}
	logger.Infof("filesystemRepository initialized")

	juggler.GlobalCache.TuneCache(
		senderConfig.CacheTTL,
		senderConfig.CacheCleanInterval,
		senderConfig.CacheCleanInterval*10,
	)
	juggler.InitEventsStore(&senderConfig.Store)

	binds := map[string]cocaine.EventHandler{
		"send": send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
