package main

import (
	"bufio"
	"bytes"
	"log"
	"os"

	"github.com/Combaine/Combaine/common"
	"github.com/Combaine/Combaine/common/tasks"
	"github.com/Combaine/Combaine/senders/razladki"
	"github.com/cocaine/cocaine-framework-go/cocaine"
)

const (
	defaultConfigPath = "/etc/combaine/razladki.conf"
)

var logger *cocaine.Logger

type Task struct {
	Id       string
	Data     tasks.DataType
	Config   razladki.RazladkiConfig
	CurrTime uint64
	PrevTime uint64
}

func getRazladkiHost() (string, error) {
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

	task.Config.Host, err = getRazladkiHost()
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

	logger.Debugf("Task: %v", task)

	rCli, err := razladki.NewRazladkiClient(&task.Config, task.Id)
	if err != nil {
		logger.Errf("Unexpected error %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = rCli.Send(task.Data, task.PrevTime)
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
