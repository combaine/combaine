package main

import (
	"bufio"
	"bytes"
	"log"
	"os"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/senders/cbb"
)

const (
	defaultConfigPath = "/etc/combaine/cbb.conf"
)

var logger *cocaine.Logger

type Task struct {
	Id       string
	Data     tasks.DataType
	Config   cbb.CBBConfig
	CurrTime uint64
	PrevTime uint64
}

func getCBBHost() (string, error) {
	var path string = os.Getenv("config")
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

func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task Task
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

	if task.Config.Host == "" {
		task.Config.Host, err = getCBBHost()
		if err != nil {
			response.ErrorMsg(-100, err.Error())
			return
		}
	}

	logger.Debugf("Task: %v", task)

	cCli, err := cbb.NewCBBClient(&task.Config, task.Id)
	if err != nil {
		logger.Errf("Unexpected error %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = cCli.Send(task.Data, task.PrevTime)
	if err != nil {
		logger.Errf("Sending error %s", err)
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
