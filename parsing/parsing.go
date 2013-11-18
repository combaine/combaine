package parsing

import (
	"fmt"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
)

type Task struct {
	Host     string
	Config   string
	Group    string
	PrevTime int
	CurrTime int
	Id       string
}

func (t *Task) String() string {
	return fmt.Sprintf("%v", t)
}

func Parsing(task Task) (err error) {
	log, err := cocaine.NewLogger()
	if err != nil {
		return
	}

	log.Info("Start ", task)
	defer log.Close()

	//Wrap it
	log.Info("Create configuration manager")
	cfgManager, err := cocaine.NewService(common.CFGMANAGER)
	defer cfgManager.Close()

	log.Info("Fetch configuration file")
	res := <-cfgManager.Call("enqueue", "parsing", task.Config)
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	var cfg common.ParsingConfig
	common.Encode(rawCfg, &cfg)

	res = <-cfgManager.Call("enqueue", "common", "")
	if err = res.Err(); err != nil {
		return
	}
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	var combainerCfg common.CombainerConfig
	common.Encode(rawCfg, &combainerCfg)

	var aggCfg common.AggConfig
	aggLogName := cfg.AggConfigs[0]
	res = <-cfgManager.Call("enqueue", "aggregate", aggLogName)
	if err = res.Err(); err != nil {
		return
	}
	if err = res.Extract(&rawCfg); err != nil {
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
		return
	}

	log.Info(fmt.Sprintf("Use %s for fetching data", fetcherType))
	fetcher, err := cocaine.NewService(fetcherType)
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
		return
	}

	var t []byte
	if err = res.Extract(&t); err != nil {
		return
	}

	// ParsingApp stage
	log.Info("Send data to parsing")
	parserApp, err := cocaine.NewService(common.PARSINGAPP)
	defer parserApp.Close()
	taskToParser, err := common.Pack([]interface{}{cfg.Parser, t})
	if err != nil {
		return
	}
	res = <-parserApp.Call("enqueue", "parse", taskToParser)
	if err = res.Err(); err != nil {
		return
	}
	var z interface{}
	if err = res.Extract(&z); err != nil {
		return
	}

	// Datagrid stage
	dgType, err := common.GetType(cfg.DG)
	if err != nil {
		return
	}

	log.Info(fmt.Sprintf("Use %s for handle data", dgType))
	datagrid, err := cocaine.NewService(dgType)
	defer datagrid.Close()
	taskToDatagrid, err := common.Pack([]interface{}{cfg.DG, z})
	if err != nil {
		return
	}
	res = <-datagrid.Call("enqueue", "put", taskToDatagrid)
	if err = res.Err(); err != nil {
		return
	}
	var token string
	if err = res.Extract(&token); err != nil {
		return
	}
	log.Info(token)
	defer func() {
		taskToDatagrid, _ = common.Pack([]interface{}{cfg.DG, token})
		<-datagrid.Call("enqueue", "drop", taskToDatagrid)
	}()

	ans := make(chan cocaine.ServiceResult)
	aggTaskCounter := 0
	for k, v := range aggCfg.Data {
		log.Info("send to ", k)
		aggType, err2 := common.GetType(v)
		if err2 != nil {
			err = err2
			return
		} else {
			task, _ := common.Pack([]interface{}{v, cfg.DG, token})
			tErr := sendMsgToAgg(aggType, task, time.Second*5, ans)
			if tErr == nil {
				aggTaskCounter++
			}
		}
	}
	for aggTaskCounter > 0 {
		select {
		case r := <-ans:
			log.Info(r.Err())
			aggTaskCounter--
		case <-time.After(time.Second * 4):
			log.Err("Deadline")
			aggTaskCounter = 0
		}
	}

	log.Info("Stop ", task)
	return nil
}

func sendMsgToAgg(name string, task []byte, deadline time.Duration, out chan cocaine.ServiceResult) (err error) {
	app, err := cocaine.NewService(name)
	if err != nil {
		return
	}
	go func() {
		defer app.Close()
		select {
		case res := <-app.Call("enqueue", "aggregate_host", task):
			out <- res
			//log.Info("Task ok")
		case <-time.After(deadline):
			//log.Err(fmt.Sprintf("Failed task %s %s", task, deadline))
		}
	}()
	return
}
