package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/senders/razladki"
)

const (
	defaultConfigPath = "/etc/combaine/razladki.conf"
)

var logger *cocaine.Logger

type Task struct {
	Id       string
	Data     tasks.DataType
	Config   razladki.RazladkiConfig
	CurrTime uint64
	PrevTime uint64
}

var razladkiHost = getRazladkiHost()

func getRazladkiHost() string {
	var path string = os.Getenv("Config")
	if len(path) == 0 {
		path = defaultConfigPath
	}

	conf, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(conf)
}

func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task Task
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	task.Config.Host = razladkiHost
	logger.Debugf("Task: %v", task)

	rCli, err := razladki.NewRazladkiClient(&task.Config, task.Id)
	if err != nil {
		logger.Errf("Unexpected error %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	err = rCli.Send(task.Data, task.PrevTime)
	if err != nil {
		logger.Errf("Sending error %s", err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
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
