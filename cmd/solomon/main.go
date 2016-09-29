package main

import (
	"bufio"
	"bytes"
	"log"
	"os"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/senders/solomon"
)

const (
	defaultConfigPath = "/etc/combaine/solomon-api.conf"
)

var (
	DEFAULT_FIELDS = []string{
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
	CONNECTION_TIMEOUT = 3000 // ms
	RW_TIMEOUT         = 5000 // ms
	logger             *cocaine.Logger
)

type Task struct {
	Id       string
	Data     tasks.DataType
	Config   solomon.SolomonCfg
	CurrTime uint64
	PrevTime uint64
}

func getApiUrl() (string, error) {
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
	logger.Debugf("Task: %v", task)

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = DEFAULT_FIELDS
	}
	if task.Config.Timeout == 0 {
		task.Config.Timeout = CONNECTION_TIMEOUT
	}
	if task.Config.Api == "" {
		task.Config.Api, err = getApiUrl()
		if err != nil {
			logger.Errf("Failed to get api url: %s", err)
			response.ErrorMsg(-100, err.Error())
			return
		}
	}

	solCli, _ := solomon.NewSolomonClient(task.Config, task.Id)
	err = solCli.Send(task.Data, task.PrevTime)
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

	go solomon.StartWorkers(solomon.JobQueue)

	Worker.Loop(binds)
}
