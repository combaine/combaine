package worker

import (
	"context"
	"sync"
	"time"

	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

// DoAggregating send tasks to cluster
func DoAggregating(ctx context.Context, task *AggregatingTask) error {
	startTm := time.Now()
	var parsingConfig = task.GetParsingConfig()
	var aggregationConfig = task.GetAggregationConfig()
	var Hosts = task.GetHosts()

	log := logrus.WithFields(logrus.Fields{
		"stage":   "DoAggregating",
		"config":  task.Config,
		"session": task.Id,
	})

	log.Infof("start")
	log.Debugf("for hosts: %v", Hosts)

	var aggWg sync.WaitGroup

	meta := parsingConfig.Metahost
	ch := make(chan *AggregationResult)

	initCap := len(aggregationConfig.Data) * len(Hosts)
	for name, cfg := range aggregationConfig.Data {
		encodedCfg, err := utils.Pack(cfg)
		if err != nil {
			log.Errorf("failed to pack config: %v", err)
			continue
		}
		aggParsingResults := make([][]byte, 0, initCap)
		aggType, err := cfg.Type()
		if err != nil {
			log.Errorf("resolve type for %s: %s", name, err)
			continue
		}
		aggClass, err := cfg.Class()
		if err != nil {
			log.Errorf("resolve %s Class for %s: %s", aggType, name, err)
			continue
		}
		log.Infof("send %s to %s.%s", name, aggType, aggClass)
		ac := NewAggregatorClient(aggregatorConnection)

		for subGroup, hosts := range Hosts {
			subGroupParsingResults := make([][]byte, 0, initCap)
			for _, host := range hosts {
				key := host + ";" + name
				data, ok := task.ParsingResult.Data[key]
				if !ok {
					log.Warnf("missing result for %s", key)
					continue
				}

				aggParsingResults = append(aggParsingResults, data)
				subGroupParsingResults = append(subGroupParsingResults, data)

				perHost, err := cfg.GetBool("perHost")
				if !perHost || err != nil {
					if err != nil {
						log.Errorf("skip per host: %s", err)
					}
					continue
				}

				log.Debugf("host %s", host)
				hostReq := &AggregateGroupRequest{
					Task: &AggregatorTask{
						Id:     task.Id,
						Config: encodedCfg,
						Meta: map[string]string{
							"type":      "host",
							"aggregate": name,
							"name":      host,
							"metahost":  meta,
						},
					},
					ClassName: aggClass,
					Payload:   [][]byte{data},
				}
				aggWg.Add(1)
				go func(r *AggregateGroupRequest) {
					defer aggWg.Done()
					res, err := ac.AggregateGroup(ctx, r)
					if err != nil {
						log.Errorf("failed to call aggregator.AggregateGroup(%s): %v", r.Task.Meta["name"], err)
					} else {
						ch <- &AggregationResult{Tags: r.Task.Meta, Result: res.Result}
					}
				}(hostReq)
			}

			if len(subGroupParsingResults) == 0 {
				log.Infof("%s %s nothing aggregate", name, subGroup)
				continue
			}

			skipPerDC, err := cfg.GetBool("skipPerDatacenter")
			if skipPerDC && err == nil {
				log.Debugf("%s %s skip by skipPerDatacenter config option", name, subGroup)
				continue
			}

			log.Debugf("group %s", subGroup)
			groupReq := &AggregateGroupRequest{
				Task: &AggregatorTask{
					Id:     task.Id,
					Config: encodedCfg,
					Meta: map[string]string{
						"type":      "datacenter",
						"aggregate": name,
						"name":      subGroup,
						"metahost":  meta,
					},
				},
				ClassName: aggClass,
				Payload:   subGroupParsingResults,
			}
			aggWg.Add(1)
			go func(r *AggregateGroupRequest) {
				defer aggWg.Done()
				res, err := ac.AggregateGroup(ctx, r)
				if err != nil {
					log.Errorf("failed to call aggregator.AggregateGroup(%s): %v", r.Task.Meta["name"], err)
				} else {
					ch <- &AggregationResult{Tags: r.Task.Meta, Result: res.Result}
				}
			}(groupReq)
		}

		if len(aggParsingResults) == 0 {
			log.Infof("%s nothing aggregate", meta)
			continue
		}

		log.Debugf("metahost %s", meta)
		metaReq := &AggregateGroupRequest{
			Task: &AggregatorTask{
				Id:     task.Id,
				Config: encodedCfg,
				Meta: map[string]string{
					"type":      "metahost",
					"aggregate": name,
					"name":      meta,
					"metahost":  meta,
				},
			},
			ClassName: aggClass,
			Payload:   aggParsingResults,
		}
		aggWg.Add(1)
		go func(r *AggregateGroupRequest) {
			defer aggWg.Done()
			res, err := ac.AggregateGroup(ctx, r)
			if err != nil {
				log.Errorf("failed to call aggregator.AggregateGroup(%s): %v", meta, err)
			} else {
				ch <- &AggregationResult{Tags: r.Task.Meta, Result: res.Result}
			}
		}(metaReq)
	}

	go func() {
		aggWg.Wait()
		close(ch)
	}()

	var result []*AggregationResult
	for item := range ch {
		result = append(result, item)
	}

	log.Infof("aggregation completed (took %.3f)", time.Now().Sub(startTm).Seconds())
	return DoSending(ctx, meta, task, aggregationConfig.Senders, result)
}
