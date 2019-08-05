package worker

import (
	"context"
	"sync"
	"time"

	"github.com/combaine/combaine/fetchers"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/sirupsen/logrus"
)

func fetchDataFromTarget(ctx context.Context, task *ParsingTask) ([]byte, error) {
	log := logrus.WithFields(logrus.Fields{
		"config":  task.ParsingConfigName,
		"target":  task.Host,
		"session": task.Id,
	})
	var parsingConfig = task.GetParsingConfig()

	fetcherType, err := parsingConfig.DataFetcher.Type()
	if err != nil {
		return nil, err
	}
	log.Debugf("use %s for fetching data", fetcherType)
	fetcher, err := fetchers.NewFetcher(fetcherType, parsingConfig.DataFetcher)
	if err != nil {
		return nil, err
	}

	fetcherTask := fetchers.FetcherTask{
		ID:     task.Id,
		Period: task.Frame.Current - task.Frame.Previous,
		Target: task.Host,
	}

	defer func(t time.Time) {
		log.Infof("fetching completed (took %.3f)", time.Now().Sub(t).Seconds())
	}(time.Now())

	blob, err := fetcher.Fetch(ctx, &fetcherTask)
	log.Debugf("fetch %d bytes: %q", len(blob), blob)
	if err != nil {
		return nil, err
	}
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

	blob, err := fetchDataFromTarget(ctx, task)
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
					log.Errorf("DoParsing resolve parser type for %s: %s", k, err)
					return
				}
				aggClass, err := v.Class()
				if err != nil {
					log.Errorf("DoParsing resolve %s Class for %s: %s", aggType, k, err)
					return
				}
				log.Debugf("DoParsing: send to '%s:%s'", aggType, aggClass)
				encodedCfg, err := utils.Pack(v)
				if err != nil {
					log.Errorf("Failed to pack task config: %v", err)
					return
				}
				req := &AggregateHostRequest{
					Task: &AggregatorTask{
						Id:     task.Id,
						Frame:  task.Frame,
						Config: encodedCfg,
						Meta: map[string]string{
							"Host": task.Host,
							"Key":  k,
						},
					},
					ClassName: aggClass,
					Payload:   blob,
				}
				key := task.Host + ";" + k
				ac := NewAggregatorClient(NextAggregatorConn())
				res, err := ac.AggregateHost(ctx, req)
				if err != nil {
					log.Errorf("Failed to call aggregator.AggregateHost: %v", err)
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
