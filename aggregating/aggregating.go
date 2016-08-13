package aggregating

import (
	"fmt"

	"github.com/Combaine/Combaine/common"
	"github.com/Combaine/Combaine/common/logger"
	"github.com/Combaine/Combaine/common/servicecacher"
	"github.com/Combaine/Combaine/common/tasks"
)

var cacher servicecacher.Cacher = servicecacher.NewCacher()

func Aggregating(task tasks.AggregationTask) error {
	logger.Infof("%s start aggregating", task.Id)
	logger.Debugf("%s aggregation config: %s", task.Id, task.AggregationConfig)
	logger.Debugf("%s aggregation hosts: %v", task.Id, task.Hosts)

	result := make(tasks.AggregationResult)

	for name, cfg := range task.AggregationConfig.Data {
		aggParsingResults := make([]map[string][]byte)
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

		for subGroup, hosts := range task.Hosts {
			subGroupParsingResults := make([]map[string][]byte)
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

				if perHost, ok = cfg["perHost"]; ok && perHost {
					payload := common.Pack(
						[]interface{}{task.Id, cfg, []tasks.ParsingResult{data}})
					res, err := enqueue(&app, payload)
					if err != nil {
						logger.Errf("%s unable to aggregate '%s' %s %s",
							task.Id, name, host, err)
						continue
					}
					result[name][host] = res
				}
			}
			payload := common.Pack([]interface{}{task.Id, cfg, subGroupParsingResults})
			res, err := enqueue(&app, payload)
			if err != nil {
				logger.Errf("%s unable to aggregate '%s' %s %s",
					task.Id, name, subGroup, err)
				continue
			}
			result[name][host] = res
		}
		payload := common.Pack([]interface{}{task.Id, cfg, aggParsingResults})
		res, err := enqueue(&app, payload)
		if err != nil {
			logger.Errf("%s unable to aggregate 'all' %s %s", task.Id, name, err)
			continue
		}
		result[name][host] = res
	}

	for name, item := range task.AggregationConfig.Senders {
		senderType, err := item.Type()
		if err != nil {
			logger.Errr("%s unable to detect sender type for %s %s",
				task.Id, name, err)
			continue
		}
		logger.Infof("%s send to sender %s", task.Id, senderType)
		app, err := cacher.Get(name)
		if err != nil {
			logger.Errf("%s skip sender %s %s %s", task.Id, senderType, name, err)
			continue
		}
		senderPayload := tasks.SenderPayload{
			Config:   item,
			Data:     result,
			CurrTime: task.CurrTime,
			PrevTime: task.PrevTime,
			Id:       task.Id,
		}

		logger.Debugf("%s data to send %v", task.Id, senderPayload)
		payload := common.Pack(senderPayload)
		res, err := enqueue(&app, payload)
		if err != nil {
			logger.Errf("%s unable to send to '%s' %s %s",
				task.Id, name, err)
		}
	}

	logger.Debugf("%s Result %v", task.Id, result)
	logger.Infof("%s Done", task.Id)

	return nil
}
