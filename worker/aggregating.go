package worker

import (
	"context"
	"sync"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func enqueue(method string, app cache.Service, payload *[]byte) (interface{}, error) {

	var rawRes interface{}

	res := <-app.Call("enqueue", method, *payload)
	if res == nil {
		return nil, common.ErrAppCall
	}
	if res.Err() != nil {
		return nil, errors.Wrap(res.Err(), "task failed")
	}

	if err := res.Extract(&rawRes); err != nil {
		return nil, errors.Wrap(err, "unable to extract result")
	}
	return rawRes, nil
}

func aggregating(t *AggregatingTask, ch chan *common.AggregationResult, res *common.AggregationResult,
	c repository.PluginConfig, d [][]byte, app cache.Service, wg *sync.WaitGroup) {

	defer wg.Done()

	payload, err := utils.Pack(common.AggregateGropuPayload{
		Task: common.Task{
			CurrTime: t.Frame.Current,
			PrevTime: t.Frame.Previous,
			Id:       t.Id,
		},
		Config: c,
		Meta:   res.Tags,
		Data:   d,
	})
	if err != nil {
		logrus.Errorf("%s unable to pack data: %s", t.Id, err)
		return
	}
	data, err := enqueue("aggregate_group", app, &payload)
	if err != nil {
		logrus.Errorf("%s unable to aggregate %s: %s", t.Id, res, err)
		return
	}
	res.Result = data
	ch <- res
}

// DoAggregating send tasks to cluster
func DoAggregating(ctx context.Context, task *AggregatingTask) error {
	startTm := time.Now()
	var parsingConfig = task.GetParsingConfig()
	var aggregationConfig = task.GetAggregationConfig()
	var Hosts = task.GetHosts()

	log := logrus.WithFields(logrus.Fields{
		"config":  task.Config,
		"session": task.Id,
	})

	log.Infof("start aggregating")
	log.Debugf("aggregation hosts: %v", Hosts)

	var aggWg sync.WaitGroup

	meta := parsingConfig.Metahost
	ch := make(chan *common.AggregationResult)

	initCap := len(aggregationConfig.Data) * len(Hosts)
	for name, cfg := range aggregationConfig.Data {
		aggParsingResults := make([][]byte, 0, initCap)
		aggType, err := cfg.Type()
		if err != nil {
			log.Errorf("unable to get aggregator type for %s: %s", name, err)
			continue
		}
		log.Infof("send %s to aggregate type %s", name, aggType)
		app, err := cacher.Get(aggType)
		if err != nil {
			log.Errorf("skip aggregator %s type %s %s", name, aggType, err)
			continue
		}

		for subGroup, hosts := range Hosts {
			subGroupParsingResults := make([][]byte, 0, initCap)
			for _, host := range hosts {
				key := host + ";" + name
				data, ok := task.ParsingResult.Data[key]
				if !ok {
					log.Warnf("unable to aggregte %s, missing result for %s", aggType, key)
					continue
				}

				aggParsingResults = append(aggParsingResults, data)
				subGroupParsingResults = append(subGroupParsingResults, data)

				perHost, err := cfg.GetBool("perHost")
				if !perHost || err != nil {
					if err != nil {
						log.Errorf("skip per host aggregating: %s", err)
					}
					continue
				}

				log.Debugf("aggregate host %s", host)
				hostAggRes := &common.AggregationResult{
					Tags: map[string]string{
						"type":      "host",
						"aggregate": name,
						"name":      host,
						"metahost":  meta,
					},
				}
				aggWg.Add(1)
				go aggregating(task, ch, hostAggRes, cfg, [][]byte{data}, app, &aggWg)
			}

			if len(subGroupParsingResults) == 0 {
				log.Infof("%s %s nothing aggregate", name, subGroup)
				continue
			}

			skipPerDC, err := cfg.GetBool("skipPerDatacenter")
			if skipPerDC && err == nil {
				continue
			}

			log.Debugf("aggregate group %s", subGroup)
			groupAggRes := &common.AggregationResult{
				Tags: map[string]string{
					"type":      "datacenter",
					"aggregate": name,
					"name":      subGroup,
					"metahost":  meta,
				},
			}
			aggWg.Add(1)
			go aggregating(task, ch, groupAggRes, cfg, subGroupParsingResults, app, &aggWg)
		}

		if len(aggParsingResults) == 0 {
			log.Infof("%s nothing aggregate", meta)
			continue
		}

		log.Debugf("aggregate metahost %s", meta)
		metaAggRes := &common.AggregationResult{
			Tags: map[string]string{
				"type":      "metahost",
				"aggregate": name,
				"name":      meta,
				"metahost":  meta,
			},
		}
		aggWg.Add(1)
		go aggregating(task, ch, metaAggRes, cfg, aggParsingResults, app, &aggWg)
	}

	go func() {
		aggWg.Wait()
		close(ch)
	}()

	var result []common.AggregationResult
	for item := range ch {
		result = append(result, *item)
	}

	log.Infof("aggregation completed (took %.3f)", time.Now().Sub(startTm).Seconds())

	startTm = time.Now()
	log.Info("start sending")
	log.Debugf("senders payload: %v", result)

	var sendersWg sync.WaitGroup
	for senderName, senderConf := range aggregationConfig.Senders {
		if _, ok := senderConf["Host"]; !ok {
			// parsing metahost as default value for plugin config
			senderConf["Host"] = meta
		}

		sendersWg.Add(1)
		go func(g *sync.WaitGroup, n string, i repository.PluginConfig) {
			defer g.Done()
			senderType, err := i.Type()
			if err != nil {
				log.Errorf("unknown sender type: %s", err)
				return
			}
			log.Infof("send to sender %s", senderType)
			app, err := cacher.Get(senderType)
			if err != nil {
				log.Errorf("skip sender %s: %s", senderType, err)
				return
			}
			senderPayload := common.SenderPayload{
				Task: common.Task{
					CurrTime: task.Frame.Current,
					PrevTime: task.Frame.Previous,
					Id:       task.Id,
				},
				Config: i,
				Data:   result,
			}

			payload, err := utils.Pack(senderPayload)
			if err != nil {
				log.Errorf("unable to pack data for %s: %s", senderType, err)
				return
			}
			res, err := enqueue("send", app, &payload)
			if err != nil {
				log.Errorf("unable to send for %s: %s", senderType, err)
				return
			}
			log.Infof("sender response for %s: %s", senderType, res)
		}(&sendersWg, senderName, senderConf)
	}
	sendersWg.Wait()
	log.Infof("senders completed (took %.3f)", time.Now().Sub(startTm).Seconds())
	return nil
}
