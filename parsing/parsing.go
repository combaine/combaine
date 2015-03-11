package parsing

import (
	"fmt"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/logger"

	"github.com/noxiouz/Combaine/common/servicecacher"
	"github.com/noxiouz/Combaine/common/tasks"
)

const (
	storageServiceName = "elliptics"
)

var (
	storage *cocaine.Service     = logger.MustCreateService(storageServiceName)
	cacher  servicecacher.Cacher = servicecacher.NewCacher()
)

func Parsing(task tasks.ParsingTask) (err error) {
	logger.Infof("%s Start parsing", task.Id)

	fetcherType, err := task.ParsingConfig.DataFetcher.Type()
	if err != nil {
		logger.Errf("%s %v", task.Id, err)
		return
	}
	logger.Debugf("%s Use %s for fetching data", task.Id, fetcherType)

	fetcher, err := NewFetcher(fetcherType, task.ParsingConfig.DataFetcher)
	if err != nil {
		logger.Errf("%s %v", task.Id, err)
		return
	}

	// Per host
	fetcherTask := tasks.FetcherTask{
		Target:     task.Host,
		CommonTask: task.CommonTask,
	}

	blob, err := fetcher.Fetch(&fetcherTask)
	if err != nil {
		logger.Errf("%s %v", task.Id, err)
		return
	}
	logger.Debugf("%s Fetch %d bytes from %s: %s", task.Id, len(blob), task.Host, blob)

	var payload interface{} = blob
	/*
		ParsingApp stage
	*/
	if !task.ParsingConfig.NeedToSkipParsingStage() {
		logger.Infof("%s Send data to parsing", task.Id)
		parser, err := GetParser()
		if err != nil {
			logger.Errf("%s %v", task.Id, err)
			return err
		}

		blob, err = parser.Parse(task.Id, task.ParsingConfig.Parser, blob)
		if err != nil {
			logger.Errf("%s %v", task.Id, err)
			return err
		}
		payload = blob
	} else {
		// Remove this logging later
		logger.Infof("%s parsing has been skipped", task.Id)
	}

	/*
		Database stage
	*/
	if !task.ParsingConfig.Raw {
		logger.Debugf("%s Use %s for handle data", task.Id, common.DATABASEAPP)
		datagrid, err := cacher.Get(common.DATABASEAPP)
		if err != nil {
			logger.Errf("%s %v", task.Id, err)
			return err
		}

		res := <-datagrid.Call("enqueue", "put", blob)
		if err = res.Err(); err != nil {
			logger.Errf("%s %v", task.Id, err)
			return err
		}
		var token string
		if err = res.Extract(&token); err != nil {
			logger.Errf("%s %v", task.Id, err)
			return err
		}

		defer func() {
			taskToDatagrid, _ := common.Pack([]interface{}{token})
			<-datagrid.Call("enqueue", "drop", taskToDatagrid)
			logger.Debugf("%s Drop table", task.Id)
		}()
		payload = token
	} else {
		logger.Infof("%s Skip DataBase stage. Raw data", task.Id)
	}

	/*

	*/
	var wg sync.WaitGroup
	for aggLogName, aggCfg := range task.AggregationConfigs {
		for k, v := range aggCfg.Data {
			aggType, err := v.Type()
			if err != nil {
				return err
			}
			logger.Debugf("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)

			wg.Add(1)
			go func(name string, k string, v interface{}, logName string, deadline time.Duration) {
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
					"id":       task.Id})

				select {
				case res := <-app.Call("enqueue", "aggregate_host", t):
					if res.Err() != nil {
						logger.Errf("%s Task failed  %s", task.Id, res.Err())
						return
					}

					var raw_res []byte
					if err := res.Extract(&raw_res); err != nil {
						logger.Errf("%s Unable to extract result. %s", task.Id, err.Error())
						return
					}

					key := fmt.Sprintf("%s;%s;%s;%s;%v",
						task.Host, task.ParsingConfigName,
						logName, k, task.CurrTime)
					<-storage.Call("cache_write", "combaine", key, raw_res)
					logger.Debugf("%s Write data with key %s", task.Id, key)
				case <-time.After(deadline):
					logger.Errf("%s Failed task %s", task.Id, deadline)
				}
			}(aggType, k, v, aggLogName, time.Second*time.Duration(task.CurrTime-task.PrevTime))
		}
	}
	wg.Wait()

	logger.Infof("%s Done", task.Id)
	return nil
}
