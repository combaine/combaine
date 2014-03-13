package main

import (
	"log"
	"runtime"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/agave"
	"github.com/noxiouz/Combaine/common"
)

var DEFAULT_FIELDS = []string{"75_prc", "90_prc", "93_prc", "94_prc", "95_prc", "96_prc", "97_prc", "98_prc", "99_prc"}

var DEFAULT_STEP = 300

type Task struct {
	Data   common.DataType
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
	logger.Debugf("Task: %v", task)
	var Items []string
	for _, item := range task.Config["items"].([]interface{}) {
		Items = append(Items, string(item.([]uint8)))
	}

	logger.Debugf("%v", task.Data)

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

	// GOVNOKOD
	fields := []string{}
	if cfgFields, ok := task.Config["Fields"]; ok {
		for _, field := range cfgFields.([]interface{}) {
			fields = append(fields, string(field.([]uint8)))
		}
	}
	// Default: empty list of strings
	if len(fields) == 0 {
		fields = DEFAULT_FIELDS
	}
	task.Config["Fields"] = fields
	logger.Debugf("Fields %v", fields)

	//step := DEFAULT_STEP
	if cfgStep, ok := task.Config["step"]; ok {
		switch cfgStep.(type) {
		case uint64:
			task.Config["step"] = int64(cfgStep.(uint64))
		case int64:
			task.Config["step"] = cfgStep.(int64)
		}
	} else {
		task.Config["step"] = int64(DEFAULT_STEP)
	}
	logger.Debugf("Step %v", task.Config["step"])

	agaveCfg := task.Config
	as, err := agave.NewAgaveSender(agaveCfg)
	if err != nil {
		logger.Errf("Unexpected error %s", err)
		response.ErrorMsg(-100, err.Error())
		response.Close()
		return
	}
	//defer as.Close()
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
