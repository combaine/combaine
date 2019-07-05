package worker

import (
	"context"
	"sync"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
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
				log.Debugf("DoParsing: send to '%s'", aggType)

				app, err := cacher.Get(aggType)
				if err != nil {
					log.Errorf("DoParsing: cacher.Get: '%s': %s", aggType, err)
					return
				}
				t, err := utils.Pack(map[string]interface{}{
					"Config": v,
					"Data":   blob,
					// TODO define task structure in common
					"Meta": map[string]string{
						"Host": task.Host,
						"Key":  k,
					},
					"PrevTime": task.Frame.Previous,
					"CurrTime": task.Frame.Current,
					"Id":       task.Id,
				})
				if err != nil {
					log.Errorf("failed to pack task: %s", err)
					return
				}

				key := task.Host + ";" + k
				select {
				case res := <-app.Call("enqueue", "aggregate_host", t):
					if res == nil {
						log.Errorf("task failed: %s", common.ErrAppCall)
						return
					}
					if res.Err() != nil {
						log.Errorf("task failed: %s", res.Err())
						return
					}

					var rawRes []byte
					if err := res.Extract(&rawRes); err != nil {
						log.Errorf("unable to extract result: %s", err.Error())
						return
					}

					ch <- item{key: key, res: rawRes}
					log.Debugf("write data with key %s", key)
				case <-ctx.Done():
					log.Errorf("failed task: %s", ctx.Err())
				}
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
