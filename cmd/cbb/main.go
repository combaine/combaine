package main

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"os"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/senders/cbb"
)

const (
	defaultConfigPath = "/etc/combaine/cbb.conf"
	defaultTimeout    = 5 * time.Second
)

type cbbTask struct {
	ID       string `codec:"Id"`
	Data     []common.AggregationResult
	Config   cbb.Config
	CurrTime uint64
	PrevTime uint64
}

func getCBBHost() (string, error) {
	var path = os.Getenv("config")
	if len(path) == 0 {
		path = defaultConfigPath
	}

	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		return string(bytes.TrimSpace(scanner.Bytes())), nil
	}

	return "", scanner.Err()
}

// Send unpack cocaine.Request and send items to cbb server
func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task cbbTask
	err := common.Unpack(raw, &task)
	if err != nil {
		logger.Errf("%s Failed to unpack CBB task %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	logger.Debugf("%s Task: %v", task.ID, task)

	if task.Config.Host == "" {
		task.Config.Host, err = getCBBHost()
		if err != nil {
			response.ErrorMsg(-100, err.Error())
			return
		}
	}

	cCli, err := cbb.NewCBBClient(&task.Config, task.ID)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	err = cCli.Send(ctx, task.Data, task.PrevTime)
	if err != nil {
		logger.Errf("%s Sending error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
	cbb.InitializeLogger()

	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
