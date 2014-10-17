package parsing

import (
	"fmt"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/servicecacher"
)

var (
	storage      *cocaine.Service
	log          *cocaine.Logger
	logMutex     sync.Mutex
	storageMutex sync.Mutex
	cacher       servicecacher.Cacher = servicecacher.NewCacher()
)

const (
	// Special parser name that allows to avoid
	// parser call
	ParserSkipValue = "NullParser"
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
func Parsing(task common.ParsingTask) (err error) {
	log, err := LazyLoggerInitialization()
	if err != nil {
		return
	}

	log.Info(task.Id, " Start parsing.")

	//Wrap it
	log.Debug(task.Id, " Create configuration manager")
	cfgManager, err := cacher.Get(common.CFGMANAGER)
	if err != nil {
		log.Errf("%s %s", task.Id, err.Error())
		return
	}
	cfgWrap := common.NewCfgWrapper(cfgManager, log)

	log.Debugf(task.Id, " Fetch configuration file ", task.Config)
	cfg, err := cfgWrap.GetParsingConfig(task.Config)
	if err != nil {
		log.Err(err.Error())
		return
	}

	combainerCfg, err := cfgWrap.GetCommon()
	if err != nil {
		log.Err(err.Error())
		return
	}

	aggCfgs := make(map[string]configs.AggregationConfig)
	for _, name := range cfg.AggConfigs {
		aggCfg, err := cfgWrap.GetAggregateConfig(name)
		if err != nil {
			log.Err(err.Error())
			return err
		}
		aggCfgs[name] = aggCfg
	}

	log.Debugf("%s Aggregate configs %s", task.Id, aggCfgs)
	common.PluginConfigsUpdate(&(combainerCfg.CloudSection.DataFetcher), &(cfg.DataFetcher))
	cfg.DataFetcher = combainerCfg.CloudSection.DataFetcher
	common.PluginConfigsUpdate(&(combainerCfg.CloudSection.DataBase), &(cfg.DataBase))
	cfg.DataBase = combainerCfg.CloudSection.DataBase

	fetcherType, err := common.GetType(cfg.DataFetcher)
	if err != nil {
		log.Err(err)
		return
	}

	log.Debugf("%s Use %s for fetching data", task.Id, fetcherType)

	fetcher, err := NewFetcher(fetcherType, cfg.DataFetcher)
	if err != nil {
		log.Err(err)
		return
	}

	// Per host

	fetcherTask := common.FetcherTask{
		Id:        task.Id,
		Target:    task.Host,
		StartTime: task.PrevTime,
		EndTime:   task.CurrTime,
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
	if cfg.Parser != ParserSkipValue && cfg.Parser != "" {
		log.Info(task.Id, " Send data to parsing")
		parser, err := GetParser()
		if err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}

		blob, err = parser.Parse(task.Id, cfg.Parser, blob)
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
		Datagrid stage
	*/
	if !cfg.Raw {
		dgType, err := common.GetType(cfg.DataBase)
		if err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}

		log.Debugf("%s Use %s for handle data", task.Id, dgType)
		datagrid, err := cacher.Get(dgType)
		if err != nil {
			log.Err(task.Id, " ", err.Error())
			return err
		}

		// taskToDatagrid, err := common.Pack(z)
		// if err != nil {
		// 	log.Err(task.Id, " ", err.Error())
		// 	return
		// }
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
	for aggLogName, aggCfg := range aggCfgs {
		for k, v := range aggCfg.Data {
			aggType, err := common.GetType(v)
			log.Debugf("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)
			if err != nil {
				return err
			}
			wg.Add(1)
			go func(name string, k string, v interface{}, deadline time.Duration) {
				defer wg.Done()
				log, err := LazyLoggerInitialization()
				storage, err := lazyStorageInitialization()
				if err != nil {
					return
				}

				app, err := cacher.Get(name) //cocaine.NewService(name)
				if err != nil {
					log.Errf("%s %s %s", task.Id, name, err)
					return
				}

				/*
					Task structure
				*/
				t, _ := common.Pack(map[string]interface{}{
					"config":   v,
					"dgconfig": cfg.DataBase,
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

					key := fmt.Sprintf("%s;%s;%s;%s;%v", task.Host, task.Config, aggLogName, k, task.CurrTime)
					<-storage.Call("cache_write", "combaine", key, raw_res)
					log.Debugf("%s Write data with key %s", task.Id, key)
				case <-time.After(deadline):
					log.Errf("%s Failed task %s", task.Id, deadline)
				}
			}(aggType, k, v, time.Second*time.Duration(task.CurrTime-task.PrevTime))
		}
	}
	wg.Wait()

	log.Info(task.Id, " Done")
	return nil
}
