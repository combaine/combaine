package worker

import (
	"context"
	"sync"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/sirupsen/logrus"
)

func fetchDataFromTarget(ctx context.Context, task *ParsingTask, parsingConfig *repository.ParsingConfig) ([]byte, error) {
	startTm := time.Now()
	log := logrus.WithFields(logrus.Fields{
		"config":  task.ParsingConfigName,
		"target":  task.Host,
		"session": task.Id,
	})
	fetcherType, err := parsingConfig.DataFetcher.Type()
	if err != nil {
		return nil, err
	}
	log.Debugf("use %s for fetching data", fetcherType)
	fetcher, err := NewFetcher(fetcherType, parsingConfig.DataFetcher)
	if err != nil {
		return nil, err
	}

	fetcherTask := common.FetcherTask{
		Target: task.Host,
		Task:   common.Task{Id: task.Id, PrevTime: task.Frame.Previous, CurrTime: task.Frame.Current},
	}

	blob, err := fetcher.Fetch(ctx, &fetcherTask)
	endTm := time.Now().Sub(startTm).Seconds()
	log.Debugf("fetch %d bytes: %q", len(blob), blob)
	if err != nil {
		log.Errorf("fetching completed (took %.3f)", endTm)
		return nil, err
	}
	log.Infof("fetching completed (took %.3f)", endTm)
	return blob, nil
}

// DoParsing distribute tasks accross cluster
func DoParsing(ctx context.Context, task *ParsingTask) (*ParsingResult, error) {
	log := logrus.WithFields(logrus.Fields{
		"config":  task.ParsingConfigName,
		"target":  task.Host,
		"session": task.Id,
	})
	log.Debugf("start parsing")

	var parsingConfig = task.GetParsingConfig()

	blob, err := fetchDataFromTarget(ctx, task, &parsingConfig)
	if err != nil {
		log.Errorf("DoParsing: %v", err)
		return nil, err
	}
	// parsing timings without fetcher time
	defer func(t time.Time) {
		log.Infof("parsing completed (took %.3f)", time.Now().Sub(t).Seconds())
	}(time.Now())

	type item struct {
		key string
		res []byte
	}
	ch := make(chan item)

	var aggregationConfigs = task.GetAggregationConfigs()
	var wg sync.WaitGroup
	for _, aggCfg := range aggregationConfigs {
		for k, v := range aggCfg.Data {
			wg.Add(1)
			go func(k string, v repository.PluginConfig) {
				defer wg.Done()

				aggType, err := v.Type()
				if err != nil {
					log.Errorf("DoParsing: %s", err)
					return
				}
				aggClass, err := v.Class()
				if err != nil {
					log.Errorf("DoParsing resolve %s Class: %s", aggType, err)
					return
				}
				log.Debugf("DoParsing: send to '%s:%s'", aggType, aggClass)

				req := &AggregateHostRequest{
					Task: &Task{
						Id:     task.Id,
						Frame:  task.Frame,
						Config: v,
						Meta: map[string]string{
							"Host": task.Host,
							"Key":  k,
						},
					},
					ClassName: aggClass,
					Payload:   blob,
				}
				key := task.Host + ";" + k
				c := NewAggregatorClient(aggregatorConnection)
				res, err := c.AggregateHost(ctx, req)
				if err != nil {
					log.Errorf("Failed to call aggregator.AggregatorHost: %v", err)
					return
				}
				log.Debugf("write data with key %s", key)
				ch <- item{key: key, res: res.GetResult()}
			}(k, v)
		}
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	result := ParsingResult{Data: make(map[string][]byte)}
	for res := range ch {
		result.Data[res.key] = res.res
	}

	return &result, nil
}
