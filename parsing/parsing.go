package parsing

import (
	"fmt"
	"sync"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"

	"github.com/combaine/combaine/common/servicecacher"
	"github.com/combaine/combaine/common/tasks"
)

func fetchDataFromTarget(task *tasks.ParsingTask) ([]byte, error) {
	fetcherType, err := task.ParsingConfig.DataFetcher.Type()
	if err != nil {
		return nil, err
	}

	logger.Debugf("%s use %s for fetching data", task.Id, fetcherType)
	fetcher, err := NewFetcher(fetcherType, task.ParsingConfig.DataFetcher)
	if err != nil {
		return nil, err
	}

	fetcherTask := tasks.FetcherTask{
		Target:     task.Host,
		CommonTask: task.CommonTask,
	}

	startTm := time.Now()
	blob, err := fetcher.Fetch(&fetcherTask)
	logger.Infof("%s fetching completed (took %.3f)", task.Id, time.Now().Sub(startTm).Seconds())
	if err != nil {
		return nil, err
	}

	logger.Debugf("%s Fetch %d bytes from %s: %s", task.Id, len(blob), task.Host, blob)
	return blob, nil
}

func parseData(task *tasks.ParsingTask, cacher servicecacher.Cacher, data []byte) ([]byte, error) {
	parser, err := GetParser(cacher)
	if err != nil {
		return nil, err
	}

	return parser.Parse(task.Id, task.ParsingConfig.Parser, data)
}

func Parsing(task *tasks.ParsingTask, cacher servicecacher.Cacher) (tasks.ParsingResult, error) {
	logger.Infof("%s start parsing %s", task.Id, task.ParsingConfigName)
	defer func(t time.Time) {
		logger.Infof("%s parsing completed (took %.3f)", task.Id, time.Now().Sub(t).Seconds())
		logger.Infof("%s %s Done", task.Id, task.ParsingConfigName)
	}(time.Now())

	var (
		blob    []byte
		err     error
		payload interface{}
		wg      sync.WaitGroup
	)

	blob, err = fetchDataFromTarget(task)
	if err != nil {
		logger.Errf("%s error `%v` occured while fetching data", task.Id, err)
		return nil, err
	}

	if !task.ParsingConfig.SkipParsingStage() {
		logger.Infof("%s Send data to parsing", task.Id)
		blob, err = parseData(task, cacher, blob)
		if err != nil {
			logger.Errf("%s error `%v` occured while parsing data", task.Id, err)
			return nil, err
		}
	}

	payload = blob

	if !task.ParsingConfig.Raw {
		logger.Infof("%s Raw data is not supported anymore", task.Id)
		return nil, fmt.Errorf("Raw data is not supported anymore")
	}

	type item struct {
		key string
		res []byte
	}
	ch := make(chan item)

	for aggLogName, aggCfg := range task.AggregationConfigs {
		for k, v := range aggCfg.Data {
			aggType, err := v.Type()
			if err != nil {
				return nil, err
			}
			logger.Debugf("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)

			wg.Add(1)
			go func(name string, k string, v interface{}, deadline time.Duration) {
				defer wg.Done()
				app, err := cacher.Get(name)
				if err != nil {
					logger.Errf("%s %s %s", task.Id, name, err)
					return
				}

				/*
					Task structure
				*/
				t, _ := common.Pack(map[string]interface{}{
					"config":   v,
					"token":    payload,
					"prevtime": task.PrevTime,
					"currtime": task.CurrTime,
					"id":       task.Id,
				})

				select {
				case res := <-app.Call("enqueue", "aggregate_host", t):
					if res == nil {
						logger.Errf("%s Task failed: %s", task.Id, common.ErrAppCall)
						return
					}
					if res.Err() != nil {
						logger.Errf("%s Task failed: %s", task.Id, res.Err())
						return
					}

					var raw_res []byte
					if err := res.Extract(&raw_res); err != nil {
						logger.Errf("%s Unable to extract result: %s", task.Id, err.Error())
						return
					}

					key := fmt.Sprintf("%s;%s", task.Host, k)
					ch <- item{key: key, res: raw_res}
					logger.Debugf("%s Write data with key %s", task.Id, key)
				case <-time.After(deadline):
					logger.Errf("%s Failed task %s", task.Id, deadline)
				}
			}(aggType, k, v, time.Second*time.Duration(task.CurrTime-task.PrevTime))
		}
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	result := make(tasks.ParsingResult)
	for res := range ch {
		result[res.key] = res.res
	}

	return result, nil
}
