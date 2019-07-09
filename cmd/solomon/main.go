package main

import (
	"bufio"
	"bytes"
	"log"
	"os"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/senders/solomon"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

const (
	defaultConfigPath = "/etc/combaine/solomon-api.conf"
	sleepInterval     = 300  // sleep after timeouts ms
	sendTimeout       = 5000 // send timeout ms
)

var (
	defaultFields = []string{
		"75_prc", "90_prc", "93_prc",
		"94_prc", "95_prc", "96_prc",
		"97_prc", "98_prc", "99_prc",
	}
)

type solomonTask struct {
	ID       string `codec:"Id"`
	Data     []common.AggregationResult
	Config   solomon.Config
	CurrTime uint64
	PrevTime uint64
}

func getAPIURL() (string, error) {
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

// Send parse cocaine request and send sensort to solomon api
func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task solomonTask
	err := utils.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	logrus.Debugf("%s Task: %v", task.ID, task)

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = defaultFields
	}
	if task.Config.Timeout == 0 {
		task.Config.Timeout = sendTimeout
	}
	if task.Config.API == "" {
		task.Config.API, err = getAPIURL()
		if err != nil {
			logrus.Errorf("%s Failed to get api url: %s", task.ID, err)
			response.ErrorMsg(-100, err.Error())
			return
		}
	}

	solCli, _ := solomon.NewSender(task.Config, task.ID)
	err = solCli.Send(task.Data, task.PrevTime)
	if err != nil {
		logrus.Errorf("%s Sending error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}

	solomon.StartWorkers(solomon.JobQueue, sleepInterval)

	Worker.Loop(binds)
}
