package main

import (
	"context"
	"log"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/senders/juggler"
	"github.com/combaine/combaine/utils"
)

var senderConfig *juggler.SenderConfig

func send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task juggler.SenderTask
	err := utils.Unpack(raw, &task)
	if err != nil {
		logger.Errf("%s Failed to unpack juggler task %s", task.Id, err)
		return
	}
	task.Config.Tags = juggler.EnsureDefaultTag(task.Config.Tags)

	err = juggler.UpdateTaskConfig(&task.Config, senderConfig)
	if err != nil {
		logger.Errf("%s Failed to update task config %s", task.Id, err)
		return
	}
	logger.Debugf("%s Task: %v", task.Id, task.Data)
	juggler.AddJugglerToken(&task.Config, senderConfig.Token)

	jCli, err := juggler.NewSender(&task.Config, task.Id)
	if err != nil {
		logger.Errf("%s send: Unexpected error %s", task.Id, err)
		return
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(task.CurrTime, 0))
	defer cancel()
	err = jCli.Send(ctx, task)
	if err != nil {
		logger.Errf("%s send: %s", task.Id, err)
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
