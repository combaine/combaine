package parsing

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"

	"github.com/combaine/combaine/common/servicecacher"

	"github.com/combaine/combaine/rpc"
)

var (
	cacher = servicecacher.NewCacher()
)

func fetchDataFromTarget(task *rpc.ParsingTask, parsingConfig *configs.ParsingConfig) ([]byte, error) {
	fetcherType, err := parsingConfig.DataFetcher.Type()
	if err != nil {
		return nil, err
	}

	logger.Debugf("%s use %s for fetching data", task.Id, fetcherType)
	fetcher, err := NewFetcher(fetcherType, parsingConfig.DataFetcher)
	if err != nil {
		return nil, err
	}

	fetcherTask := tasks.FetcherTask{
		Target:     task.Host,
		CommonTask: tasks.CommonTask{Id: task.Id, PrevTime: task.Frame.Previous, CurrTime: task.Frame.Current},
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

func parseData(id string, name string, data []byte) ([]byte, error) {
	parser, err := GetParser(cacher)
	if err != nil {
		return nil, err
	}

	return parser.Parse(id, name, data)
}

func DoParsing(ctx context.Context, task *rpc.ParsingTask) (*rpc.ParsingResult, error) {
	logger.Infof("%s start parsing", task.Id)

	var parsingConfig configs.ParsingConfig

	blob, err := fetchDataFromTarget(task, &parsingConfig)
	// parsing timings without fetcher time
	defer func(t time.Time) {
		logger.Infof("%s parsing completed (took %.3f)", task.Id, time.Now().Sub(t).Seconds())
		logger.Infof("%s %s Done", task.Id, task.ParsingConfigName)
	}(time.Now())
	if err != nil {
		logger.Errf("%s error `%v` occured while fetching data", task.Id, err)
		return nil, err
	}

	if !parsingConfig.SkipParsingStage() {
		logger.Infof("%s Send data to parsing", task.Id)
		blob, err = parseData(task.Id, parsingConfig.Parser, blob)
		if err != nil {
			logger.Errf("%s error `%v` occured while parsing data", task.Id, err)
			return nil, err
		}
	}

	if !parsingConfig.Raw {
		logger.Infof("%s Raw data is not supported anymore", task.Id)
		return nil, fmt.Errorf("Raw data is not supported anymore")
	}

	type item struct {
		key string
		res []byte
	}
	ch := make(chan item)

	var aggregationConfigs map[string]configs.AggregationConfig
	var wg sync.WaitGroup
	for aggLogName, aggCfg := range aggregationConfigs {
		for k, v := range aggCfg.Data {
			aggType, err := v.Type()
			if err != nil {
				return nil, err
			}
			logger.Debugf("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)

			app, err := cacher.Get(aggType)
			if err != nil {
				logger.Errf("%s %s %s", task.Id, aggType, err)
				continue
			}
			wg.Add(1)
			// TODO: use Context instead of deadline
			go func(app servicecacher.Service, k string, v interface{}, deadline time.Duration) {
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
						logger.Errf("%s Task failed: %s", task.Id, common.ErrAppCall)
						return
					}
					if res.Err() != nil {
						logger.Errf("%s Task failed: %s", task.Id, res.Err())
						return
					}

					var rawRes []byte
					if err := res.Extract(&rawRes); err != nil {
						logger.Errf("%s Unable to extract result: %s", task.Id, err.Error())
						return
					}

					key := fmt.Sprintf("%s;%s", task.Host, k)
					ch <- item{key: key, res: raw_res}
					logger.Debugf("%s Write data with key %s", task.Id, key)
				case <-time.After(deadline):
					logger.Errf("%s Failed task %s", task.Id, deadline)
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
		result[res.key] = res.res
	}

	return &result, nil
}
