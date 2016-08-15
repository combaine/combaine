package aggregating

import (
	"fmt"
	"sync"

	"github.com/Combaine/Combaine/common"
	"github.com/Combaine/Combaine/common/configs"
	"github.com/Combaine/Combaine/common/logger"
	"github.com/Combaine/Combaine/common/servicecacher"
	"github.com/Combaine/Combaine/common/tasks"
	"github.com/cocaine/cocaine-framework-go/cocaine"
)

type list []interface{}
type item struct {
	agg  string
	name string
	res  []byte
}

var cacher servicecacher.Cacher = servicecacher.NewCacher()

func Aggregating(task *tasks.AggregationTask) error {
	logger.Infof("%s start aggregating", task.Id)
	logger.Debugf("%s aggregation config: %s", task.Id, task.AggregationConfig)
	logger.Debugf("%s aggregation hosts: %v", task.Id, task.Hosts)

	result := make(tasks.AggregationResult)
	ch := make(chan item)
	var aggWg sync.WaitGroup

	initCap := len(task.AggregationConfig.Data) * len(task.Hosts)
	for name, cfg := range task.AggregationConfig.Data {
		aggParsingResults := make(list, 0, initCap)
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
			subGroupParsingResults := make(list, 0, initCap)
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

				aggWg.Add(1)
				go func(an string, rn string, cfg configs.PluginConfig, data *list,
					app *cocaine.Service, wg *sync.WaitGroup) {
					defer wg.Done()

					payload, _ := common.Pack(list{task.Id, cfg, *data})
					res, err := enqueue(app, payload)
					if err != nil {
						logger.Errf("%s unable to aggregate '%s' %s %s",
							task.Id, an, rn, err)
						return
					}
					ch <- item{agg: an, name: rn, res: res}
				}(name, host, cfg, &list{data}, app, &aggWg)
			}
			aggWg.Add(1)
			go func(an string, rn string, cfg configs.PluginConfig, data *list,
				app *cocaine.Service, wg *sync.WaitGroup) {
				defer wg.Done()

				payload, _ := common.Pack(list{task.Id, cfg, *data})
				res, err := enqueue(app, payload)
				if err != nil {
					logger.Errf("%s unable to aggregate '%s' %s %s",
						task.Id, an, rn, err)
					return
				}
				ch <- item{agg: an, name: rn, res: res}
			}(name, subGroup, cfg, &subGroupParsingResults, app, &aggWg)
		}
		aggWg.Add(1)
		go func(an string, rn string, cfg configs.PluginConfig, data *list,
			app *cocaine.Service, wg *sync.WaitGroup) {
			defer wg.Done()

			payload, _ := common.Pack(list{task.Id, cfg, *data})
			res, err := enqueue(app, payload)
			if err != nil {
				logger.Errf("%s unable to aggregate 'all' %s %s", task.Id, an, err)
				return
			}
			ch <- item{agg: an, name: rn, res: res}
		}(name, task.ParsingConfig.Metahost, cfg, &aggParsingResults, app, &aggWg)
	}

	go func() {
		aggWg.Wait()
		close(ch)
	}()

	for item := range ch {
		result[item.agg][item.name] = item.res
	}

	var sendersWg sync.WaitGroup
	for name, item := range task.AggregationConfig.Senders {
		sendersWg.Add(1)
		go func(g *sync.WaitGroup, n string, i configs.PluginConfig) {
			defer g.Done()
			senderType, err := i.Type()
			if err != nil {
				logger.Errf("%s unknown sender type '%s' %s", task.Id, n, err)
				return
			}
			logger.Infof("%s send to sender %s", task.Id, senderType)
			app, err := cacher.Get(n)
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
				logger.Errf("%s unable to send %s %s", task.Id, n, err)
			}
			logger.Infof("%s Sender response %s %v", task.Id, n, res)
		}(&sendersWg, name, item)
	}
	sendersWg.Wait()

	logger.Debugf("%s Result %v", task.Id, result)
	logger.Infof("%s Done", task.Id)

	return nil
}
