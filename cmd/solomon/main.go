package main

import (
	"bufio"
	"bytes"
	"context"
	"os"

	"github.com/combaine/combaine/senders"
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
	Data     []senders.AggregationResult
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

type sender struct{}

// DoSend repack request and send sensort to solomon api
func (*sender) DoSend(ctx context.Context, req *senders.SenderRequest) (*senders.SenderResponse, error) {
	log := logrus.WithFields(logrus.Fields{"session": req.Id})

	var task solomonTask
	err := utils.Unpack(req.Config, &task.Config)
	if err != nil {
		return nil, err
	}
	log.Debugf("Task: %v", task)

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = defaultFields
	}
	if task.Config.Timeout == 0 {
		task.Config.Timeout = sendTimeout
	}
	if task.Config.API == "" {
		task.Config.API, err = getAPIURL()
		if err != nil {
			log.Errorf("Failed to get api url: %s", err)
			return nil, err
		}
	}

	solCli, _ := solomon.NewSender(task.Config, req.Id)
	err = solCli.Send(task.Data, task.PrevTime)
	if err != nil {
		log.Errorf("Sending error %s", err)
		return nil, err
	}
	return &senders.SenderResponse{Response: "Ok"}, nil
}

func main() {
	solomon.StartWorkers(solomon.JobQueue, sleepInterval)
	// TODO
}
