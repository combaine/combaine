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

func Parsing(task common.ParsingTask) (err error) {
	log, err := lazyLoggerInitialization()
	log, err = lazyLoggerInitialization()
	if err != nil {
		return
	}

	log.Info(task.Id, " Start")
	//defer log.Close()

	//Wrap it
	log.Info(task.Id, " Create configuration manager")
	cfgManager, err := cocaine.NewService(common.CFGMANAGER)
	if err != nil {
		log.Err(err.Error())
		return
	}
	defer cfgManager.Close()

	log.Info(task.Id, " Fetch configuration file ", task.Config)
	res := <-cfgManager.Call("enqueue", "parsing", task.Config)
	if err = res.Err(); err != nil {
		log.Err(task.Id, err.Error())
		return err
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		log.Err(task.Id, err)
		return
	}
	var cfg common.ParsingConfig
	common.Encode(rawCfg, &cfg)

	res = <-cfgManager.Call("enqueue", "common", "")
	if err = res.Err(); err != nil {
		log.Err(err)
		return
	}
	if err = res.Extract(&rawCfg); err != nil {
		log.Err(err)
		return
	}
	var combainerCfg common.CombainerConfig
	common.Encode(rawCfg, &combainerCfg)

	var aggCfg common.AggConfig
	aggLogName := cfg.AggConfigs[0]
	res = <-cfgManager.Call("enqueue", "aggregate", aggLogName)
	if err = res.Err(); err != nil {
		log.Err(err)
		return
	}
	if err = res.Extract(&rawCfg); err != nil {
		log.Err(err)
		return
	}
	common.Encode(rawCfg, &aggCfg)
	log.Info(aggCfg.Data)

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

	log.Info(fmt.Sprintf("%s Use %s for fetching data", task.Id, fetcherType))
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

	res = <-fetcher.Call("enqueue", "get", js)
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

	log.Info(fmt.Sprintf("%s Use %s for handle data", task.Id, dgType))
	datagrid, err := cocaine.NewService(dgType)
	if err != nil {
		log.Err(task.Id, " ", err.Error())
		return
	}
	defer func() {
		log.Err("Try close mysqldg connection")
		datagrid.Close()
		log.Err("Close mysqldg connection")
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
	log.Info(token)
	defer func() {
		taskToDatagrid, _ = common.Pack([]interface{}{cfg.DG, token})
		<-datagrid.Call("enqueue", "drop", taskToDatagrid)
		log.Err("Drop table")
	}()

	var wg sync.WaitGroup
	for k, v := range aggCfg.Data {
		aggType, err2 := common.GetType(v)
		log.Info(task.Id, fmt.Sprintf(" Send to %s type %s %v", k, aggType, v))
		if err2 != nil {
			err = err2
			return
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
					log.Info(task.Id, " ", name, err.Error())
					return
				}
				defer app.Close()
				t, _ := common.Pack([]interface{}{v, cfg.DG, token, task.PrevTime, task.CurrTime})

				select {
				case res := <-app.Call("enqueue", "aggregate_host", t):
					log.Info("Task ok", res)
					if res.Err() != nil {
						return
					}
					var raw_res []byte
					err = res.Extract(&raw_res)
					if err != nil {
						return
					}
					key := fmt.Sprintf("%s;%s;%s;%s;%v", task.Host, task.Config, aggLogName, k, task.CurrTime)
					log.Info("Key ", key, " ", raw_res)
					_, _ = <-storage.Call("cache_write", "combaine", key, raw_res)
					log.Info("Key write ", key)
				case <-time.After(deadline):
					log.Err(fmt.Sprintf("Failed task %s %s", task.Id, deadline))
				}
			}(aggType, k, v, time.Second*5)
		}
	}
	wg.Wait()

	log.Info(task.Id, " Stop")
	return nil
}
