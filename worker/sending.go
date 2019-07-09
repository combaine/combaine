package worker

import (
	"context"
	"sync"
	"time"

	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

// DoSending pass aggregated data to senders
func DoSending(ctx context.Context, meta string, task *AggregatingTask, senders map[string]repository.PluginConfig, payload []*AggregationResult) error {
	log := logrus.WithFields(logrus.Fields{
		"stage":   "DoSending",
		"config":  task.Config,
		"session": task.Id,
	})

	defer func(t time.Time) {
		log.Infof("senders completed (took %.3f)", time.Now().Sub(t).Seconds())
	}(time.Now())

	log.Info("start sending")
	log.Debugf("senders payload: %v", payload)

	var wg sync.WaitGroup
	for name, conf := range senders {
		if _, ok := conf["Host"]; !ok {
			// parsing metahost as default value for plugin config
			conf["Host"] = meta
		}
		senderType, err := conf.Type()
		if err != nil {
			log.Errorf("unknown sender type for section %s: %s", name, err)
			continue
		}
		sc, err := GetSenderClient(senderType)
		if err != nil {
			log.Errorf("skip sender %s.%s: %s", name, senderType, err)
			continue
		}
		encodedConf, err := utils.Pack(conf)
		if err != nil {
			log.Error("failed to pack sender config %s.%s: %s", name, senderType, err)
		}

		wg.Add(1)
		go func(sc SenderClient, n string, t string, c []byte) {
			log.Infof("send to sender %s.%s", name, senderType)
			defer wg.Done()
			req := &SenderRequest{
				Id:     task.Id,
				Frame:  task.Frame,
				Config: c,
				Data:   payload,
			}

			r, err := sc.DoSend(ctx, req)
			if err != nil {
				log.Errorf("unable to send for %s.%s: %s", name, senderType, err)
				return
			}
			log.Infof("sender response for %s.%s: %s", name, senderType, r.GetResponse())
		}(sc, name, senderType, encodedConf)
	}
	wg.Wait()
	return nil
}
