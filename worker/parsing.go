package worker

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"

	"github.com/combaine/combaine/rpc"
)

func fetchDataFromTarget(task *rpc.ParsingTask, parsingConfig *common.ParsingConfig) ([]byte, error) {
	fetcherType, err := parsingConfig.DataFetcher.Type()
	if err != nil {
		return nil, err
	}

	logrus.Debugf("%s use %s for fetching data", task.Id, fetcherType)
	fetcher, err := NewFetcher(fetcherType, parsingConfig.DataFetcher)
	if err != nil {
		return nil, err
	}

	fetcherTask := common.FetcherTask{
		Target: task.Host,
		Task:   common.Task{Id: task.Id, PrevTime: task.Frame.Previous, CurrTime: task.Frame.Current},
	}

	startTm := time.Now()
	blob, err := fetcher.Fetch(&fetcherTask)
	logrus.Infof("%s fetching completed (took %.3f)", task.Id, time.Now().Sub(startTm).Seconds())
	if err != nil {
		return nil, err
	}

	logrus.Debugf("%s Fetch %d bytes from %s: %q", task.Id, len(blob), task.Host, blob)
	return blob, nil
}

// DoParsing distribute tasks accross cluster
func DoParsing(ctx context.Context, task *rpc.ParsingTask, cacher cache.ServiceCacher) (*rpc.ParsingResult, error) {
	logrus.Infof("%s start parsing", task.Id)

	var parsingConfig = task.GetParsingConfig()

	blob, err := fetchDataFromTarget(task, &parsingConfig)
	// parsing timings without fetcher time
	defer func(t time.Time) {
		logrus.Infof("%s parsing completed (took %.3f)", task.Id, time.Now().Sub(t).Seconds())
		logrus.Infof("%s %s Done", task.Id, task.ParsingConfigName)
	}(time.Now())
	if err != nil {
		logrus.Errorf("%s error `%v` occured while fetching data", task.Id, err)
		return nil, err
	}

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
			logrus.Debugf("%s Send to %s, agg section name %s type %s", task.Id, aggLogName, k, aggType)

			app, err := cacher.Get(aggType)
			if err != nil {
				logrus.Errorf("%s %s %s", task.Id, aggType, err)
				continue
			}
			wg.Add(1)
			// TODO: use Context instead of deadline
			go func(app cache.Service, k string, v interface{}, deadline time.Duration) {
				defer wg.Done()

				/* Task structure */
				t, _ := common.Pack(map[string]interface{}{
					"config":   v,
					"token":    blob,
					"prevtime": task.Frame.Previous,
					"currtime": task.Frame.Current,
					"id":       task.Id,
				})

				select {
				case res := <-app.Call("enqueue", "aggregate_host", t):
					if res == nil {
						logrus.Errorf("%s Task failed: %s", task.Id, common.ErrAppCall)
						return
					}
					if res.Err() != nil {
						logrus.Errorf("%s Task failed: %s", task.Id, res.Err())
						return
					}

					var rawRes []byte
					if err := res.Extract(&rawRes); err != nil {
						logrus.Errorf("%s Unable to extract result: %s", task.Id, err.Error())
						return
					}

					key := fmt.Sprintf("%s;%s", task.Host, k)
					ch <- item{key: key, res: rawRes}
					logrus.Debugf("%s Write data with key %s", task.Id, key)
				case <-time.After(deadline):
					logrus.Errorf("%s Failed task %s", task.Id, deadline)
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
