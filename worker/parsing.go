package worker

import (
	"context"
	"sync"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/repository"
	"github.com/sirupsen/logrus"

	"github.com/combaine/combaine/rpc"
)

func fetchDataFromTarget(log *logrus.Entry, task *rpc.ParsingTask, parsingConfig *repository.ParsingConfig) ([]byte, error) {
	startTm := time.Now()
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

	blob, err := fetcher.Fetch(&fetcherTask)
	log.Debugf("fetch %d bytes: %q", len(blob), blob)
	log.Infof("fetching completed (took %.3f)", time.Now().Sub(startTm).Seconds())
	if err != nil {
		return nil, err
	}
	return blob, nil
}

// DoParsing distribute tasks accross cluster
func DoParsing(ctx context.Context, task *rpc.ParsingTask, cacher cache.ServiceCacher) (*rpc.ParsingResult, error) {
	log := logrus.WithFields(logrus.Fields{
		"config":  task.ParsingConfigName,
		"target":  task.Host,
		"session": task.Id,
	})
	log.Debugf("start parsing")

	var parsingConfig = task.GetParsingConfig()

	blob, err := fetchDataFromTarget(log, task, &parsingConfig)
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
	for aggLogName, aggCfg := range aggregationConfigs {
		for k, v := range aggCfg.Data {
			aggType, err := v.Type()
			if err != nil {
				return nil, err
			}
			log.Debugf("DoParsing: send to '%s', agg section name '%s' type '%s'", aggLogName, k, aggType)

			app, err := cacher.Get(aggType)
			if err != nil {
				log.Errorf("DoParsing: cacher.Get: '%s': %s", aggType, err)
				continue
			}
			wg.Add(1)
			// TODO: use Context instead of deadline
			go func(app cache.Service, k string, v repository.PluginConfig, deadline time.Duration) {
				defer wg.Done()

				t, err := common.Pack(map[string]interface{}{
					"Config": v,
					"Data":   blob,
					// TODO define task structure in common
					//"Meta": map[string]string{
					//	"Host": task.Host,
					//	"Key":  k,
					//},
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
				case <-time.After(deadline):
					log.Errorf("failed task %s: DeadlineExceeded", key)
				}
			}(app, k, v, time.Second*time.Duration(task.Frame.Current-task.Frame.Previous))
		}
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	result := rpc.ParsingResult{Data: make(map[string][]byte)}
	for res := range ch {
		result.Data[res.key] = res.res
	}

	return &result, nil
}
