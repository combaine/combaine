package parsing

import (
	"fmt"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
)

/*
1. Fetch data DONE
2. Send to parsing
3. Send to datagrid application
4. Call aggregators
*/

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
	log := cocaine.NewLogger()
	log.Info("Start ", task)
	defer log.Close()

	//Wrap it
	log.Debug("Create configuration manager")
	cfgManager := cocaine.NewService(common.CFGMANAGER)
	defer cfgManager.Close()

	log.Debug("Fetch configuration file")
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
	fetcher := cocaine.NewService(fetcherType)
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
	parserApp := cocaine.NewService(common.PARSINGAPP)
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
	datagrid := cocaine.NewService(dgType)
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

	log.Info("Stop ", task)
	return nil
}
