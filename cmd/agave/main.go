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
	defaultFields       = []string{"75_prc", "90_prc", "93_prc", "94_prc", "95_prc", "96_prc", "97_prc", "98_prc", "99_prc"}
	defaultStep   int64 = 300
)

type agaveTask struct {
	ID     string
	Data   []tasks.AggregationResult
	Config agave.Config
}

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

// Send parse cocaine Requset and send all metrics to agave hosts
func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()

	var task agaveTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	logger.Debugf("%s Task: %v", task.ID, task)

	task.Config.ID = task.ID
	task.Config.Hosts, err = getAgaveHosts()
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = defaultFields
	}

	if task.Config.Step == 0 {
		task.Config.Step = defaultStep
	}

	logger.Debugf("%s Fields: %v Step: %d", task.ID, task.Config.Fields, task.Config.Step)

	as, err := agave.NewSender(task.Config)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.ID, err)
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
