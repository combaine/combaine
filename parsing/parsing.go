package parsing

import (
	"encoding/json"
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
	cfgManager := cocaine.NewService("cfgmanager")
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

	fetcherType, err := common.GetType(cfg.DF)
	if err != nil {
		return
	}

	fetcher := cocaine.NewService(fetcherType)
	defer fetcher.Close()

	cfg.DF["host"] = task.Host
	cfg.DF["StartTime"] = task.CurrTime - task.PrevTime
	js, _ := json.Marshal(cfg.DF)

	res = <-fetcher.Call("enqueue", "get", js)
	if err = res.Err(); err != nil {
		return
	}

	var t []byte
	if err = res.Extract(&t); err != nil {
		return
	}

	log.Info("Stop ", task)
	return nil
}
