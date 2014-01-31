package main

import (
	"fmt"
	"log"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/plugins/timetail"
)

var (
	logger *cocaine.Logger
)

func CfgandTaskUrl(cfg map[string]interface{}, task *common.FetcherTask) string {
	url := fmt.Sprintf("http://%s:%v%s%s&time=%d",
		task.Target,
		cfg["timetail_port"],
		cfg["timetail_url"],
		cfg["logname"],
		task.EndTime-task.StartTime)
	logger.Info(url)
	return url
}

func get(request *cocaine.Request, response *cocaine.Response) {
	incoming := <-request.Read()

	var inp struct {
		Config map[string]interface{} "Config"
		Task   common.FetcherTask     "Task"
	}

	err := common.Unpack(incoming, &inp)
	if err != nil {
		logger.Err(err)
		response.ErrorMsg(1, fmt.Sprintf("%v", err))
		response.Close()
		return
	}

	url := CfgandTaskUrl(inp.Config, &inp.Task)
	res, err := timetail.Get(url)
	if err != nil {
		response.ErrorMsg(1, fmt.Sprintf("%v", err))
		response.Close()
		return
	}

	response.Write(res)
	response.Close()
}

func main() {
	var err error
	logger, err = cocaine.NewLogger()
	if err != nil {
		log.Fatal(err)
	}
	binds := map[string]cocaine.EventHandler{
		"get": get,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
