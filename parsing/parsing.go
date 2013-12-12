package parsing

import (
	"fmt"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
)

var (
	storage      *cocaine.Service
	log          *cocaine.Logger
	logMutex     sync.Mutex
	storageMutex sync.Mutex
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

// Wrapper around cfgmanager. TBD: move to common
type cfgWrapper struct {
	cfgManager *cocaine.Service
	log        *cocaine.Logger
}

func (m *cfgWrapper) GetParsingConfig(name string) (cfg common.ParsingConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "parsing", name)
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	err = common.Encode(rawCfg, &cfg)
	return
}

func (m *cfgWrapper) GetAggregateConfig(name string) (cfg common.AggConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "aggregate", name)
	if err = res.Err(); err != nil {
		log.Err(err)
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		log.Err(err)
		return
	}

	err = common.Encode(rawCfg, &cfg)
	return
}

func (m *cfgWrapper) GetCommon() (combainerCfg common.CombainerConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "common", "")
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	err = common.Encode(rawCfg, &combainerCfg)
	return
}

func (m *cfgWrapper) Close() {
	m.cfgManager.Close()
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
	log.Info(task.Id, " Create configuration manager")
	cfgManager, err := cocaine.NewService(common.CFGMANAGER)
	if err != nil {
		log.Errf("%s %s", task.Id, err.Error())
		return
	}
	cfgWrap := cfgWrapper{cfgManager, log}
	defer cfgWrap.Close()

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
	fetcher, err := cocaine.NewService(fetcherType)
	if err != nil {
		log.Err(err.Error())
		return
	}
	defer fetcher.Close()

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
	parserApp, err := cocaine.NewService(common.PARSINGAPP)
	if err != nil {
		log.Err(err.Error())
		return
	}
	defer parserApp.Close()
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
	datagrid, err := cocaine.NewService(dgType)
	if err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}
	defer func() {
		datagrid.Close()
		log.Errf("%s %s", task.Id, "Close mysqldg connection")
	}()

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
		log.Infof("%s Drop table", task.Id)
	}()

	var wg sync.WaitGroup
	for aggLogName, aggCfg := range aggCfgs {
		for k, v := range aggCfg.Data {
			aggType, err := common.GetType(v)
			log.Infof("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)
			if err != nil {
				return err
			} else {
				wg.Add(1)
				go func(name string, k string, v interface{}, deadline time.Duration) {
					defer wg.Done()
					log, err := lazyLoggerInitialization()
					storage, err := lazyStorageInitialization()
					if err != nil {
						return
					}

					app, err := cocaine.NewService(name)
					if err != nil {
						log.Infof("%s %s %s", task.Id, name, err)
						return
					}
					defer app.Close()
					t, _ := common.Pack([]interface{}{v, cfg.DG, token, task.PrevTime, task.CurrTime})

					select {
					case res := <-app.Call("enqueue", "aggregate_host", t):
						if res.Err() != nil {
							log.Errf("%s Task failed  %s", task.Id, res.Err())
							return
						}

						var raw_res []byte
						err = res.Extract(&raw_res)
						if err != nil {
							log.Errf("%s Unable to extract result. %s", task.Id, err.Error())
							return
						}
						key := fmt.Sprintf("%s;%s;%s;%s;%v", task.Host, task.Config, aggLogName, k, task.CurrTime)
						//log.Info("Key ", key, " ", raw_res)
						<-storage.Call("cache_write", "combaine", key, raw_res)
						log.Infof("%s Write data with key %s", task.Id, key)
					case <-time.After(deadline):
						log.Errf("%s Failed task %s", task.Id, deadline)
					}
				}(aggType, k, v, time.Second*5)
			}
		}
	}
	wg.Wait()

	log.Info(task.Id, " Done")
	return nil
}
