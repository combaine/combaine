package aggregating

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/rpc"
)

type item struct {
	agg  string
	name string
	res  interface{}
}

func enqueue(method string, app servicecacher.Service, payload *[]byte) (interface{}, error) {

	var raw_res interface{}

	res := <-app.Call("enqueue", method, *payload)
	if res == nil {
		return nil, common.ErrAppCall
	}
	if res.Err() != nil {
		return nil, fmt.Errorf("task failed  %s", res.Err())
	}

	if err := res.Extract(&raw_res); err != nil {
		return nil, fmt.Errorf("unable to extract result. %s", err.Error())
	}
	return raw_res, nil
}

func aggregating(id string, ch chan item, agg string, h string, c configs.PluginConfig, d []interface{},
	app servicecacher.Service, wg *sync.WaitGroup) {
	defer wg.Done()

	payload, _ := common.Pack([]interface{}{id, c, d})
	res, err := enqueue("aggregate_group", app, &payload)
	if err != nil {
		logger.Errf("%s %s %s unable to aggregate: %s", id, h, agg, err)
		return
	}
	ch <- item{agg: agg, name: h, res: res}
}

func Do(ctx context.Context, task *rpc.AggregatingTask, cacher servicecacher.Cacher) error {
	startTm := time.Now()
	var parsingConfig = task.GetParsingConfig()
	var aggregationConfig = task.GetAggregationConfig()
	var Hosts = task.GetHosts()

	logger.Infof("%s start aggregating %s", task.Id, task.Config)
	logger.Debugf("%s aggregation config: %s", task.Id, aggregationConfig)
	logger.Debugf("%s aggregation hosts: %v", task.Id, Hosts)

	var aggWg sync.WaitGroup

	meta := parsingConfig.Metahost
	result := make(tasks.AggregationResult)
	ch := make(chan item)

	initCap := len(aggregationConfig.Data) * len(Hosts)
	for name, cfg := range aggregationConfig.Data {
		aggParsingResults := make([]interface{}, 0, initCap)
		aggType, err := cfg.Type()
		if err != nil {
			logger.Errf("%s unable to detect aggregator type for %s %s %s", task.Id, task.Config, name, err)
			continue
		}
		logger.Infof("%s send %s %s to aggregate type %s", task.Id, task.Config, name, aggType)
		app, err := cacher.Get(aggType)
		if err != nil {
			logger.Errf("%s skip %s aggregator %s type %s %s", task.Id, task.Config, name, aggType, err)
			continue
		}

		for subGroup, hosts := range Hosts {
			subGroupParsingResults := make([]interface{}, 0, initCap)
			for _, host := range hosts {
				key := fmt.Sprintf("%s;%s", host, name)
				data, ok := task.ParsingResult.Data[key]
				if !ok {
					logger.Warnf("%s unable to aggregte %s, missing result for %s", task.Id, aggType, key)
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

				logger.Debugf("%s %s data to aggregate host %s: %v", task.Id, task.Config, host, data)
				aggWg.Add(1)
				go aggregating(task.Id, ch, name, host, cfg, []interface{}{data}, app, &aggWg)
			}

			if len(subGroupParsingResults) == 0 {
				logger.Infof("%s %s %s nothing aggregate", task.Id, name, subGroup)
				continue
			}

			logger.Debugf("%s %s data to aggregate group %s: %v", task.Id, task.Config, subGroup, subGroupParsingResults)
			aggWg.Add(1)
			go aggregating(task.Id, ch, name, meta+"-"+subGroup, cfg, subGroupParsingResults, app, &aggWg)
		}

		if len(aggParsingResults) == 0 {
			logger.Infof("%s %s %s nothing aggregate", task.Id, task.Config, meta)
			continue
		}

		logger.Debugf("%s %s data to aggregate metahost %s: %v", task.Id, task.Config, meta, aggParsingResults)
		aggWg.Add(1)
		go aggregating(task.Id, ch, name, meta, cfg, aggParsingResults, app, &aggWg)
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

	logger.Infof("%s aggregation completed (took %.3f)", task.Id, time.Now().Sub(startTm).Seconds())

	startTm = time.Now()
	logger.Infof("%s %s start sending", task.Id, task.Config)

	var sendersWg sync.WaitGroup
	for name, item := range aggregationConfig.Senders {
		if _, ok := item["Host"]; !ok {
			// parsing metahost as default value for plugin config
			item["Host"] = meta
		}

		sendersWg.Add(1)
		go func(g *sync.WaitGroup, n string, i configs.PluginConfig) {
			defer g.Done()
			senderType, err := i.Type()
			if err != nil {
				logger.Errf("%s %s unknown sender type %s", task.Id, task.Config, err)
				return
			}
			logger.Infof("%s send to sender %s", task.Id, senderType)
			app, err := cacher.Get(senderType)
			if err != nil {
				logger.Errf("%s %s skip sender %s %s", task.Id, task.Config, senderType, err)
				return
			}
			senderPayload := tasks.SenderPayload{
				CommonTask: tasks.CommonTask{
					CurrTime: task.Frame.Current,
					PrevTime: task.Frame.Previous,
					Id:       task.Id,
				},
				Config: i,
				Data:   result,
			}

			logger.Debugf("%s %s %s data to send %s", task.Id, task.Config, senderType, senderPayload)
			payload, _ := common.Pack(senderPayload)
			res, err := enqueue("send", app, &payload)
			if err != nil {
				logger.Errf("%s %s %s unable to send %s", task.Id, task.Config, senderType, err)
			}
			logger.Infof("%s %s %s sender response %s", task.Id, task.Config, senderType, res)
		}(&sendersWg, name, item)
	}
	sendersWg.Wait()

	logger.Debugf("%s result %s", task.Id, result)
	logger.Infof("%s senders completed (took %.3f)", task.Id, time.Now().Sub(startTm).Seconds())
	logger.Infof("%s %s Done", task.Id, task.Config)

	return nil
}
