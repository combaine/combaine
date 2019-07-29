package senders

import (
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

// Payload for sender.
type Payload struct {
	Tags   map[string]string
	Result interface{} // various sender payloads
}

// SenderTask ...
type SenderTask struct {
	PrevTime int64
	CurrTime int64
	Data     []*Payload
}

// RepackSenderRequest to internal representation
func RepackSenderRequest(req *SenderRequest) (*SenderTask, error) {
	log := logrus.WithFields(logrus.Fields{"session": req.Id})

	task := SenderTask{
		PrevTime: req.PrevTime,
		CurrTime: req.CurrTime,
		Data:     make([]*Payload, 0, len(req.Data)),
	}

	// Repack sender task
	for _, d := range req.Data {
		p := Payload{Tags: d.Tags}
		if err := utils.Unpack(d.Result, &p.Result); err != nil {
			log.Errorf("Failed to unpack sender payload: %s", err)
			continue
		}
		task.Data = append(task.Data, &p)
	}
	return &task, nil
}
