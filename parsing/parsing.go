package parsing

import (
	"fmt"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/servicecacher"
)

var (
	storage      *cocaine.Service
	log          *cocaine.Logger
	logMutex     sync.Mutex
	storageMutex sync.Mutex
	cacher       servicecacher.Cacher = servicecacher.NewCacher()
)

func lazyLoggerInitialization() (*cocaine.Logger, error) {
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
	log, err := lazyLoggerInitialization()
	log, err = lazyLoggerInitialization()
	if err != nil {
		return
	}

	log.Info(task.Id, " Start parsing.")

	//Wrap it
	log.Debug(task.Id, " Create configuration manager")
	// cfgManager, err := cocaine.NewService(common.CFGMANAGER)
	cfgManager, err := cacher.Get(common.CFGMANAGER)
	if err != nil {
		log.Errf("%s %s", task.Id, err.Error())
		return
	}
	cfgWrap := common.NewCfgWrapper(cfgManager, log)
	//defer cfgWrap.Close()

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

	aggCfgs := make(map[string]common.AggConfig)
	for _, name := range cfg.AggConfigs {
		aggCfg, err := cfgWrap.GetAggregateConfig(name)
		if err != nil {
			log.Err(err.Error())
			return err
		}
		aggCfgs[name] = aggCfg
	}
	log.Debugf("%s Aggregate configs %s", task.Id, aggCfgs)

	common.MapUpdate(&(combainerCfg.CloudCfg.DF), &(cfg.DF))
	cfg.DF = combainerCfg.CloudCfg.DF
	common.MapUpdate(&(combainerCfg.CloudCfg.DS), &(cfg.DS))
	cfg.DS = combainerCfg.CloudCfg.DS
	common.MapUpdate(&(combainerCfg.CloudCfg.DG), &(cfg.DG))
	cfg.DG = combainerCfg.CloudCfg.DG

	fetcherType, err := common.GetType(cfg.DF)
	if err != nil {
		log.Err(err)
		return
	}

	log.Debugf("%s Use %s for fetching data", task.Id, fetcherType)
	fetcher, err := cacher.Get(fetcherType)
	if err != nil {
		log.Err(err.Error())
		return
	}
	//defer fetcher.Close()

	fetcherTask := common.FetcherTask{
		Target:    task.Host,
		StartTime: task.PrevTime,
		EndTime:   task.CurrTime,
	}

	ft := struct {
		Config map[string]interface{} "Config"
		Task   common.FetcherTask     "Task"
	}{cfg.DF, fetcherTask}

	js, _ := common.Pack(ft)

	res := <-fetcher.Call("enqueue", "get", js)
	if err = res.Err(); err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}

	var t []byte
	if err = res.Extract(&t); err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}

	// ParsingApp stage
	log.Info(task.Id, " Send data to parsing")
	parserApp, err := cacher.Get(common.PARSINGAPP)
	if err != nil {
		log.Err(err.Error())
		return
	}
	// defer parserApp.Close()
	taskToParser, err := common.Pack([]interface{}{cfg.Parser, t})
	if err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}
	res = <-parserApp.Call("enqueue", "parse", taskToParser)
	if err = res.Err(); err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}
	var z interface{}
	if err = res.Extract(&z); err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}

	// Datagrid stage
	dgType, err := common.GetType(cfg.DG)
	if err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}

	log.Debugf("%s Use %s for handle data", task.Id, dgType)
	datagrid, err := cacher.Get(dgType)
	if err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}
	// defer func() {
	// 	datagrid.Close()
	// 	log.Errf("%s %s", task.Id, "Close mysqldg connection")
	// }()

	taskToDatagrid, err := common.Pack([]interface{}{cfg.DG, z})
	if err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}
	res = <-datagrid.Call("enqueue", "put", taskToDatagrid)
	if err = res.Err(); err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}
	var token string
	if err = res.Extract(&token); err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}

	defer func() {
		taskToDatagrid, _ = common.Pack([]interface{}{cfg.DG, token})
		<-datagrid.Call("enqueue", "drop", taskToDatagrid)
		log.Debugf("%s Drop table", task.Id)
	}()

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
				log, err := lazyLoggerInitialization()
				storage, err := lazyStorageInitialization()
				if err != nil {
					return
				}

				app, err := cacher.Get(name) //cocaine.NewService(name)
				if err != nil {
					log.Errf("%s %s %s", task.Id, name, err)
					return
				}
				//defer app.Close()

				/*
					Task structure
				*/
				t, _ := common.Pack(map[string]interface{}{
					"config":   v,
					"dgconfig": cfg.DG,
					"token":    token,
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
