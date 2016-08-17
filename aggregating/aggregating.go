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

type item struct {
	agg  string
	name string
	res  interface{}
}

func enqueue(method string, app servicecacher.Service, payload *[]byte,
	id string) (interface{}, error) {

	var raw_res interface{}

	res := <-app.Call("enqueue", method, *payload)
	if res == nil {
		return nil, fmt.Errorf("%s failed to call app method '%s'", id, method)
	}
	if res.Err() != nil {
		return nil, fmt.Errorf("%s task failed  %s", id, res.Err())
	}

	if err := res.Extract(&raw_res); err != nil {
		return nil, fmt.Errorf("%s unable to extract result. %s", id, err.Error())
	}
	return raw_res, nil
}

func aggregating(id string, ch chan item, agg string, h string,
	c configs.PluginConfig, d []interface{},
	app servicecacher.Service, wg *sync.WaitGroup) {
	defer wg.Done()

	payload, _ := common.Pack([]interface{}{id, c, d})
	res, err := enqueue("aggregate_group", app, &payload, id)
	if err != nil {
		logger.Errf("%s %s '%s' unable to aggregate %s", id, h, agg, err)
		return
	}
	ch <- item{agg: agg, name: h, res: res}
}

func Aggregating(task *tasks.AggregationTask, cacher servicecacher.Cacher) error {
	logger.Infof("%s start aggregating", task.Id)
	logger.Debugf("%s aggregation config: %s", task.Id, task.AggregationConfig)
	logger.Debugf("%s aggregation hosts: %v", task.Id, task.Hosts)

	var aggWg sync.WaitGroup

	meta := task.ParsingConfig.Metahost
	result := make(tasks.AggregationResult)
	ch := make(chan item)

	initCap := len(task.AggregationConfig.Data) * len(task.Hosts)
	for name, cfg := range task.AggregationConfig.Data {
		aggParsingResults := make([]interface{}, 0, initCap)
		aggType, err := cfg.Type()
		if err != nil {
			logger.Errf("%s unable to detect aggregator type for %s %s",
				task.Id, name, err)
			continue
		}
		logger.Infof("%s send %s to aggregate type %s", task.Id, name, aggType)
		app, err := cacher.Get(aggType)
		if err != nil {
			logger.Errf("%s skip %s aggregator %s %s", task.Id, name, aggType, err)
			continue
		}

		for subGroup, hosts := range task.Hosts {
			subGroupParsingResults := make([]interface{}, 0, initCap)
			for _, host := range hosts {
				key := fmt.Sprintf("%s;%s;%s;%s;%v", host,
					task.ParsingConfigName, task.Config, name, task.CurrTime)

				data, ok := task.ParsingResult[key]
				if !ok {
					logger.Warnf("%s %s '%s' unable to aggregte, missing result for '%s'",
						task.Id, name, host, key)
					continue
				}

				aggParsingResults = append(aggParsingResults, data)
				subGroupParsingResults = append(subGroupParsingResults, data)

				perHost, ok := cfg["perHost"]
				if !ok {
					continue
				}
				v, ok := perHost.(bool)
				if !ok {
					logger.Errf("%s 'perHost' support only bool value", task.Id)
					continue
				}
				if !v {
					continue
				}

				aggWg.Add(1)
				go aggregating(task.Id, ch, name, host,
					cfg, []interface{}{data}, app, &aggWg)
			}
			if len(subGroupParsingResults) == 0 {
				logger.Infof("%s %s '%s' nothing aggregate", task.Id, name, subGroup)
				continue
			}
			aggWg.Add(1)
			go aggregating(task.Id, ch, name, meta+"-"+subGroup,
				cfg, subGroupParsingResults, app, &aggWg)
		}
		if len(aggParsingResults) == 0 {
			logger.Infof("%s %s 'all' nothing aggregate", task.Id, name)
			continue
		}

		aggWg.Add(1)
		go aggregating(task.Id, ch, name, meta,
			cfg, aggParsingResults, app, &aggWg)
	}

	go func() {
		aggWg.Wait()
		close(ch)
	}()

	for item := range ch {
		if _, ok := result[item.agg]; !ok {
			result[item.agg] = make(tasks.ParsingResult)
		}
		result[item.agg][item.name] = item.res
	}

	var sendersWg sync.WaitGroup
	for name, item := range task.AggregationConfig.Senders {
		sendersWg.Add(1)
		go func(g *sync.WaitGroup, n string, i configs.PluginConfig) {
			defer g.Done()
			senderType, err := i.Type()
			if err != nil {
				logger.Errf("%s %s unknown sender type %s", task.Id, n, err)
				return
			}
			logger.Infof("%s send to sender %s", task.Id, senderType)
			app, err := cacher.Get(senderType)
			if err != nil {
				logger.Errf("%s skip sender '%s' %s %s", task.Id, senderType, n, err)
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

			logger.Debugf("%s %s data to send %s", task.Id, senderType, senderPayload)
			payload, _ := common.Pack(senderPayload)
			res, err := enqueue("send", app, &payload, task.Id)
			if err != nil {
				logger.Errf("%s %s '%s' unable to send %s", task.Id, n, senderType, err)
			}
			logger.Infof("%s %s '%s' sender response %s", task.Id, n, senderType, res)
		}(&sendersWg, name, item)
	}
	sendersWg.Wait()

	logger.Debugf("%s result %s", task.Id, result)
	logger.Infof("%s done", task.Id)

	return nil
}
