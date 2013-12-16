package main

import (
	"log"
	"runtime"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/plugins/senders/agave"
)

type Task struct {
	Data   agave.DataType
	Config map[string]interface{}
}

var logger *cocaine.Logger

func Send(request *cocaine.Request, response *cocaine.Response) {
	raw := <-request.Read()
	//var task map[string]map[string]interface{}
	var task Task
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	logger.Infof("%v", task)
	var Items []string
	for _, item := range task.Config["items"].([]interface{}) {
		Items = append(Items, string(item.([]uint8)))
	}

	logger.Errf("%v", task.Data)

	cfgManager, err := cocaine.NewService(common.CFGMANAGER)
	if err != nil {
		logger.Errf("%s", err.Error())
		return
	}
	defer cfgManager.Close()

	res := <-cfgManager.Call("enqueue", "common", "")
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	var combainerCfg common.CombainerConfig
	err = common.Encode(rawCfg, &combainerCfg)

	// Rewrite this shit to struct
	task.Config["items"] = Items
	task.Config["hosts"] = combainerCfg.CloudCfg.Agave
	task.Config["graph_name"] = string(task.Config["graph_name"].([]uint8))
	task.Config["graph_template"] = string(task.Config["graph_template"].([]uint8))
	agaveCfg := task.Config
	as, err := agave.NewAgaveSender(agaveCfg)
	if err != nil {
		logger.Errf("Unexpected error %s", err)
		response.ErrorMsg(-100, err.Error())
	}
	as.Send(task.Data)
	response.Write("OK")
	response.Close()
}

func main() {
	runtime.GOMAXPROCS(10)
	var err error
	logger, err = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
