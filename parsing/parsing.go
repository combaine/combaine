package parsing

import (
	"fmt"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/servicecacher"
	"github.com/noxiouz/Combaine/common/tasks"
)

var (
	storage      *cocaine.Service
	log          *cocaine.Logger
	logMutex     sync.Mutex
	storageMutex sync.Mutex
	cacher       servicecacher.Cacher = servicecacher.NewCacher()
)

func LazyLoggerInitialization() (*cocaine.Logger, error) {
	var err error
	if log != nil {
		return log, nil
	} else {
		logMutex.Lock()
		defer logMutex.Unlock()
		if log != nil {
			return log, nil
		}
		log, err = cocaine.NewLogger()
		return log, err
	}
}

func lazyStorageInitialization() (*cocaine.Service, error) {
	var err error

	if storage != nil {
		return storage, nil
	} else {
		storageMutex.Lock()
		defer storageMutex.Unlock()
		if storage != nil {
			return storage, nil
		}
		storage, err = cocaine.NewService("elliptics")
		return storage, err
	}
}

// Main parsing function
func Parsing(task tasks.ParsingTask) (err error) {
	log, err := LazyLoggerInitialization()
	if err != nil {
		return
	}
	log.Info(task.Id, " Start parsing")

	fetcherType, err := task.ParsingConfig.DataFetcher.Type()
	if err != nil {
		log.Err(err)
		return
	}
	log.Debugf("%s Use %s for fetching data", task.Id, fetcherType)

	fetcher, err := NewFetcher(fetcherType, task.ParsingConfig.DataFetcher)
	if err != nil {
		log.Err(err)
		return
	}

	// Per host
	fetcherTask := tasks.FetcherTask{
		Target:     task.Host,
		CommonTask: task.CommonTask,
	}

	blob, err := fetcher.Fetch(&fetcherTask)
	if err != nil {
		log.Err(err)
		return
	}
	log.Debugf("%s Fetch %d bytes", task.Id, len(blob))

	var payload interface{} = blob
	/*
		ParsingApp stage
	*/
	if !task.ParsingConfig.NeedToSkipParsingStage() {
		log.Info(task.Id, " Send data to parsing")
		parser, err := GetParser()
		if err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}

		blob, err = parser.Parse(task.Id, task.ParsingConfig.Parser, blob)
		if err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}
		payload = blob
	} else {
		// Remove this logging later
		log.Info(task.Id, " parsing has been skipped")
	}

	/*
		Database stage
	*/
	if !task.ParsingConfig.Raw {
		log.Debugf("%s Use %s for handle data", task.Id, common.DATABASEAPP)
		datagrid, err := cacher.Get(common.DATABASEAPP)
		if err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}

		res := <-datagrid.Call("enqueue", "put", blob)
		if err = res.Err(); err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}
		var token string
		if err = res.Extract(&token); err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}

		defer func() {
			taskToDatagrid, _ := common.Pack([]interface{}{token})
			<-datagrid.Call("enqueue", "drop", taskToDatagrid)
			log.Debugf("%s Drop table", task.Id)
		}()
		payload = token
	} else {
		log.Info(task.Id, " Skip DataBase stage. Raw data")
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
			log.Debugf("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)

			wg.Add(1)
			go func(name string, k string, v interface{}, logName string, deadline time.Duration) {
				defer wg.Done()
				log, err := LazyLoggerInitialization()
				storage, err := lazyStorageInitialization()
				if err != nil {
					return
				}

				app, err := cacher.Get(name)
				if err != nil {
					log.Errf("%s %s %s", task.Id, name, err)
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
						log.Errf("%s Task failed  %s", task.Id, res.Err())
						return
					}

					var raw_res []byte
					if err := res.Extract(&raw_res); err != nil {
						log.Errf("%s Unable to extract result. %s", task.Id, err.Error())
						return
					}

					key := fmt.Sprintf("%s;%s;%s;%s;%v",
						task.Host, task.ParsingConfigName,
						logName, k, task.CurrTime)
					<-storage.Call("cache_write", "combaine", key, raw_res)
					log.Debugf("%s Write data with key %s", task.Id, key)
				case <-time.After(deadline):
					log.Errf("%s Failed task %s", task.Id, deadline)
				}
			}(aggType, k, v, aggLogName, time.Second*time.Duration(task.CurrTime-task.PrevTime))
		}
	}
	wg.Wait()

	log.Info(task.Id, " Done")
	return nil
}
