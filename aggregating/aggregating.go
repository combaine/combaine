package aggregating

import (
	"fmt"
	"sync"

	"github.com/Combaine/Combaine/common"
	"github.com/Combaine/Combaine/common/configs"
	"github.com/Combaine/Combaine/common/logger"
	"github.com/Combaine/Combaine/common/servicecacher"
	"github.com/Combaine/Combaine/common/tasks"
)

type list []interface{}

var cacher servicecacher.Cacher = servicecacher.NewCacher()

func Aggregating(task tasks.AggregationTask) error {
	logger.Infof("%s start aggregating", task.Id)
	logger.Debugf("%s aggregation config: %s", task.Id, task.AggregationConfig)
	logger.Debugf("%s aggregation hosts: %v", task.Id, task.Hosts)

	result := make(tasks.AggregationResult)

	initialCapacity := len(task.AggregationConfig.Data)
	for name, cfg := range task.AggregationConfig.Data {
		aggParsingResults := make(list, 0, initialCapacity)
		aggType, err := cfg.Type()
		if err != nil {
			logger.Errf("%s unable to detect aggregator type for %s %s",
				task.Id, name, err)
			continue
		}
		logger.Infof("%s send %s to aggregate type %s", task.Id, name, aggType)
		app, err := cacher.Get(name)
		if err != nil {
			logger.Errf("%s skip aggregator %s %s %s", task.Id, aggType, name, err)
			continue
		}

		hostsLen := len(task.Hosts)
		for subGroup, hosts := range task.Hosts {
			subGroupParsingResults := make(list, 0, hostsLen)
			for _, host := range hosts {
				key := fmt.Sprintf("%s;%s;%s;%s;%v", host,
					task.ParsingConfigName, task.Config, name, task.CurrTime)

				data, ok := task.ParsingResult[key]
				if !ok {
					logger.Errf("%s unable to aggregte %s %s, no key '%s'",
						task.Id, name, host, key)
					continue
				}

				aggParsingResults = append(aggParsingResults, data)
				subGroupParsingResults = append(subGroupParsingResults, data)

				if perHost, ok := cfg["perHost"]; ok {
					if v, ok := perHost.(bool); ok && !v {
						continue
					}
				} else {
					continue
				}

				payload, _ := common.Pack(list{task.Id, cfg, list{data}})
				res, err := enqueue(app, payload)
				if err != nil {
					logger.Errf("%s unable to aggregate '%s' %s %s",
						task.Id, name, host, err)
					continue
				}
				result[name][host] = res
			}
			payload, _ := common.Pack(list{task.Id, cfg, subGroupParsingResults})
			res, err := enqueue(app, payload)
			if err != nil {
				logger.Errf("%s unable to aggregate '%s' %s %s",
					task.Id, name, subGroup, err)
				continue
			}
			result[name][subGroup] = res
		}
		payload, _ := common.Pack(list{task.Id, cfg, aggParsingResults})
		res, err := enqueue(app, payload)
		if err != nil {
			logger.Errf("%s unable to aggregate 'all' %s %s", task.Id, name, err)
			continue
		}
		result[name][task.ParsingConfig.Metahost] = res
	}

	var sendersWg sync.WaitGroup
	for name, item := range task.AggregationConfig.Senders {
		sendersWg.Add(1)
		go func(g *sync.WaitGroup, n string, i configs.PluginConfig) {
			defer g.Done()
			senderType, err := i.Type()
			if err != nil {
				logger.Errf("%s unable to detect sender type for %s %s",
					task.Id, n, err)
				return
			}
			logger.Infof("%s send to sender %s", task.Id, senderType)
			app, err := cacher.Get(n)
			defer func() {
				if app != nil {
					app.Close()
				}
			}()
			if err != nil {
				logger.Errf("%s skip sender %s %s %s", task.Id, senderType, n, err)
				return
			}
			senderPayload := tasks.SenderPayload{
				CommonTask: tasks.CommonTask{
					CurrTime: task.CurrTime,
					PrevTime: task.PrevTime,
					Id:       task.Id,
				},
				Config: i,
				Data:   result,
			}

			logger.Debugf("%s data to send %v", task.Id, senderPayload)
			payload, _ := common.Pack(senderPayload)
			res, err := enqueue(app, payload)
			if err != nil {
				logger.Errf("%s unable to send to '%s' %s %s",
					task.Id, n, err)
			}
			logger.Infof("%s Sender response %v", task.Id, res)
		}(&sendersWg, name, item)
	}
	sendersWg.Wait()

	logger.Debugf("%s Result %v", task.Id, result)
	logger.Infof("%s Done", task.Id)

	return nil
}
