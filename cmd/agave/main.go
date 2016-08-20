package main

import (
	"bufio"
	"bytes"
	"log"
	"os"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
	"github.com/combaine/combaine/senders/agave"
)

var (
	DEFAULT_FIELDS       = []string{"75_prc", "90_prc", "93_prc", "94_prc", "95_prc", "96_prc", "97_prc", "98_prc", "99_prc"}
	DEFAULT_STEP   int64 = 300
)

func getAgaveHosts() ([]string, error) {
	var path = os.Getenv("config")
	if len(path) == 0 {
		path = "/etc/combaine/agave.conf"
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		result = append(result, string(bytes.TrimSpace(scanner.Bytes())))
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

type Task struct {
	Id     string
	Data   tasks.DataType
	Config agave.AgaveConfig
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
	logger.Debugf("%s Task: %v", task.Id, task)

	task.Config.Id = task.Id
	task.Config.Hosts, err = getAgaveHosts()
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

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
		return
	}
	as.Send(task.Data)
	response.Write("OK")
}

func main() {
	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
