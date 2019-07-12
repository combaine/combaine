package main

import (
	"context"

	"github.com/combaine/combaine/senders"
	"github.com/combaine/combaine/senders/graphite"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

var (
	defaultFields = []string{
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
	defaultConnectionEndpoint = ":42000"
)

type graphiteTask struct {
	Data     []senders.AggregationResult
	Config   graphite.Config
	CurrTime uint64
	PrevTime uint64
}

type sender struct{}

// DoSend repack request and send points to graphite
func (*sender) DoSend(ctx context.Context, req *senders.SenderRequest) (*senders.SenderResponse, error) {
	log := logrus.WithFields(logrus.Fields{"session": req.Id})

	var task graphiteTask
	err := utils.Unpack(req, &task)
	if err != nil {
		log.Errorf("Failed to unpack graphite task %s", err)
		return
	}
	log.Debugf("Task: %v", task)

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = defaultFields
	}
	if task.Config.Endpoint == "" {
		task.Config.Endpoint = defaultConnectionEndpoint
	}

	gCli, err := graphite.NewSender(&task.Config, task.ID)
	if err != nil {
		log.Errf("Unexpected error %s", err)
		return
	}

	err = gCli.Send(task.Data, task.PrevTime)
	if err != nil {
		log.Errf("Sending error %s", err)
		return
	}
	return &senders.SenderResponse{Response: "Ok"}, nil
}

func main() {
	// TODO
}
