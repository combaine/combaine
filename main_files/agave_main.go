package main

import (
	"log"
	"runtime"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/senders/agave"
)

var (
	DEFAULT_FIELDS       = []string{"75_prc", "90_prc", "93_prc", "94_prc", "95_prc", "96_prc", "97_prc", "98_prc", "99_prc"}
	DEFAULT_STEP   int64 = 300
)

type Task struct {
	Id     string
	Data   tasks.DataType
	Config agave.AgaveConfig
}

func Send(request *cocaine.Request, response *cocaine.Response) {
	raw := <-request.Read()

	var task Task
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	logger.Debugf("%s Task: %v", task.Id, task)

	cfgManager, err := cocaine.NewService(common.CFGMANAGER)
	if err != nil {
		logger.Errf("%s, %s", task.Id, err.Error())
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
	var combainerCfg configs.CombainerConfig
	err = common.Encode(rawCfg, &combainerCfg)

	task.Config.Id = task.Id
	task.Config.Hosts = combainerCfg.CloudSection.AgaveHosts

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = DEFAULT_FIELDS
	}

	if task.Config.Step == 0 {
		task.Config.Step = DEFAULT_STEP
	}

	logger.Debugf("%s Fields: %v Step: %d", task.Id, task.Config.Fields, task.Config.Step)

	as, err := agave.NewAgaveSender(task.Config)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.Id, err)
		response.ErrorMsg(-100, err.Error())
		response.Close()
		return
	}
	as.Send(task.Data)
	response.Write("OK")
	response.Close()
}

func main() {
	runtime.GOMAXPROCS(2)
	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
